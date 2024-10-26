package converter

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"imersaofc/internal/rabbitmq"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

type VideoConverter struct {
	db             *sql.DB
	rabbitmqClient *rabbitmq.RabbitClient
}

func NewVideoConverter(rabbitmqClient *rabbitmq.RabbitClient, db *sql.DB) *VideoConverter {
	return &VideoConverter{
		rabbitmqClient: rabbitmqClient,
		db:             db,
	}
}

// baseado no json enviado {"video_id": 1, "path": "media/uploads/1"}
type VideoTask struct {
	VideoId int    `json:"video_id"`
	Path    string `json:"path"`
}

// * = ponteiro, qualquer valor que for alterado utilizando vc. vai ser refletido no codigo
func (vc *VideoConverter) Handle(d amqp.Delivery, conversionExch, confirmationKey, confirmationQueue string) {
	var task VideoTask

	//& = quando o comando executar o task alterar na memoria o valor
	err := json.Unmarshal(d.Body, &task)

	if err != nil {
		vc.logError(task, "Failed to unmarshal task", err)
		return
	}

	if IsProcessed(vc.db, task.VideoId) {
		slog.Warn("Video already processed", slog.Int("video_id", task.VideoId))
		d.Ack(false)
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
		return
	}
	d.Ack(false)
	slog.Info("Video marked as processed", slog.Int("video_id", task.VideoId))

	confirmationMessage := []byte(fmt.Sprintf(`{"video_id": %d, "path":"%s"}`, task.VideoId, task.Path))
	err = vc.rabbitmqClient.PublishMessage(conversionExch, confirmationKey, confirmationQueue, confirmationMessage)
}

func (vc *VideoConverter) processVideo(task *VideoTask) error {
	mergedFile := filepath.Join(task.Path, "merged.mp4")
	mpegDashPath := filepath.Join(task.Path, "mpeg-dash")

	// Merge chunks
	slog.Info("Merging chunks", slog.String("path", task.Path))
	if err := vc.mergeChunks(task.Path, mergedFile); err != nil {
		return fmt.Errorf("failed to merge chunks: %v", err)
	}

	// Create directory for MPEG-DASH output
	if err := os.MkdirAll(mpegDashPath, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create output directory: %v", err)
	}

	// Convert to MPEG-DASH
	ffmpegCmd := exec.Command(
		"ffmpeg", "-i", mergedFile, // Arquivo de entrada
		"-f", "dash", // Formato de saída
		filepath.Join(mpegDashPath, "output.mpd"), // Caminho para salvar o arquivo .mpd
	)
	output, err := ffmpegCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to convert to MPEG-DASH: %v, output: %s", err, string(output))
	}
	slog.Info("Converted to MPEG-DASH", slog.String("path", mpegDashPath))
	// Remove merged file after processing
	if err := os.Remove(mergedFile); err != nil {
		slog.Warn("Failed to remove merged file", slog.String("file", mergedFile), slog.String("error", err.Error()))
	}
	slog.Info("Removed merged file", slog.String("file", mergedFile))
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

func (vc *VideoConverter) mergeChunks(inputDir, outputFile string) error {
	// Buscar todos os arquivos .chunk no diretório
	chunks, err := filepath.Glob(filepath.Join(inputDir, "*.chunk"))
	if err != nil {
		return fmt.Errorf("failed to find chunks: %v", err)
	}

	//Slice = array que pode aumentar de capacidade
	//Ordenacao da lista que iremos trabalhar
	sort.Slice(chunks, func(i, j int) bool {
		//numero atual que esta e vai comparar se o i for menor que o extracNumber retorna true e nao muda a posicao, caso contrario muda
		return vc.extractNumber(chunks[i]) < vc.extractNumber(chunks[j])
	})

	//criando arquivo de saida
	output, err := os.Create(outputFile)
	if err != nil {
		return fmt.Errorf("failed to create merged file: %v", err)
	}

	//statement - fecha o arquivo depois que a funcao terminar de rodar
	defer output.Close()

	// _ = indice (blank identify)
	for _, chunk := range chunks {
		// abrindo arquivo chunk
		input, err := os.Open(chunk)
		if err != nil {
			return fmt.Errorf("failed to open chunk %s: %v", chunk, err)
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
