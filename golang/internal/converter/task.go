package converter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"
)

type VideoConverter struct {
	db *sql.DB
}

func NewVideoConverter(db *sql.DB) *VideoConverter {
	return &VideoConverter{
		db: db,
	}
}

// baseado no json enviado {"video_id": 1, "path": "media/uploads/1"}
type VideoTask struct {
	VideoId int    `json:"video_id"`
	Path    string `json:"path"`
}

// * = ponteiro, qualquer valor que for alterado utilizando vc. vai ser refletido no codigo
func (vc *VideoConverter) Handle(msg []byte) {
	var task VideoTask

	//& = quando o comando executar o task alterar na memoria o valor
	err := json.Unmarshal(msg, &task)

	if err != nil {
		vc.logError(task, "Failed to unmarshal task", err)
		return
	}

	if IsProcessed(vc.db, task.VideoId) {
		slog.Warn("Video already processed", slog.Int("video_id", task.VideoId))
		return
	}

	err = vc.processVideo(&task)
	if err != nil {
		vc.logError(task, "Failed to process video", err)
		return
	}

	// Mark as processed
	err = MarkProcessed(vc.db, task.VideoId)
	if err != nil {
		vc.logError(task, "Failed to mark video as processed", err)
	}
	slog.Info("Video marked as processed", slog.Int("video_id", task.VideoId))
}

func (vc *VideoConverter) processVideo(task *VideoTask) error {
	mergedFile := filepath.Join(task.Path, "merged.mp4")
	mpegDashPath := filepath.Join(task.Path, "mpeg-dash")

	slog.Info("Merging chunks", slog.String("path", task.Path))
	err := vc.mergeChunks(task.Path, mergedFile)
	if err != nil {
		vc.logError(*task, "Failed to merge chunks", err)
		return err
	}
	slog.Info("Creating mpeg-dash dir", slog.String("path", task.Path))
	err = os.MkdirAll(mpegDashPath, os.ModePerm)
	if err != nil {
		vc.logError(*task, "Failed to create mpeg-dash directory", err)
		return err
	}

	slog.Info("Converting video to mpeg-dash", slog.String("path", task.Path))
	ffmpegCmd := exec.Command(
		"ffmpeg", "-i", mergedFile,
		"-f", "dash",
		filepath.Join(mpegDashPath, "output.mpd"),
	)

	output, err := ffmpegCmd.CombinedOutput()
	if err != nil {
		vc.logError(*task, "Failed to convert video to mpeg-dash, output"+string(output), err)
		return err
	}

	slog.Info("Video converted to mpeg-dash", slog.String("path", mpegDashPath))

	slog.Info("Removing merged file", slog.String("path", mergedFile))
	err = os.Remove(mergedFile)
	if err != nil {
		vc.logError(*task, "Failed to remove merged file", err)
		return err
	}

	return nil
}

func (vc *VideoConverter) logError(task VideoTask, message string, err error) {
	errorData := map[string]any{
		"video_id": task.VideoId,
		"error":    message,
		"details":  err.Error(),
		"time":     time.Now(),
	}

	serializedError, _ := json.Marshal(errorData)
	slog.Error("Processing error", slog.String("error_details", string(serializedError)))

	//todo register error on database
	RegisterError(vc.db, errorData, err)
}

func (vc *VideoConverter) extractNumber(fileName string) int {
	re := regexp.MustCompile(`\d+`)
	numStr := re.FindString(filepath.Base(fileName)) //string converter para inteiro
	// converte de string para inteiro
	num, err := strconv.Atoi(numStr)
	if err != nil {
		return -1
	}
	return num
}

func (vc *VideoConverter) mergeChunks(intputDir, outputFile string) error {
	chunks, err := filepath.Glob(filepath.Join(intputDir, "*.chunk"))
	if err != nil {
		return fmt.Errorf("failed to find chunks: %v", err)
	}

	//Slice = array que pode aumentar de capacidade
	//Ordenacao da lista que iremos trabalhar
	sort.Slice(chunks, func(i, j int) bool {
		//numero atual que esta e vai comparar se o i for menor que o extracNumber retorna true e nao muda a posicao, caso contrario muda
		return vc.extractNumber(chunks[i]) < vc.extractNumber((chunks[j]))
	})

	//criando arquivo de saida
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create output file: %v", err)
	}

	//statement - fecha o arquivo depois que a funcao terminar de rodar
	defer output.Close()

	// _ = indice (blank identify)
	for _, chunk := range chunks {
		// abrindo arquivo chunk
		input, err := os.Open(chunk)
		if err != nil {
			return fmt.Errorf("failed to open chunk: %v", err)
		}

		// _ = nao quero usar como resultado, quero que apenas faca a copia
		_, err = output.ReadFrom(input)
		if err != nil {
			return fmt.Errorf("failed to write chunk %s to merged file: %v", chunk, err)
		}
		input.Close()
	}

	return nil
}
