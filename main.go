package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Fact представляет данные для отправки на сервер.
type Fact struct {
	PeriodStart         string
	PeriodEnd           string
	PeriodKey           string
	IndicatorToMoID     int
	IndicatorToMoFactID int
	Value               int
	FactTime            string
	IsPlan              bool
	AuthUserID          int
	Comment             string
}

// Buffer управляет очередью фактов и их обработкой.
type Buffer struct {
	mu    sync.Mutex
	queue []Fact
	wg    sync.WaitGroup
}

// NewBuffer инициализирует буфер.
func NewBuffer() *Buffer {
	return &Buffer{}
}

// AddFacts добавляет факты в очередь и запускает обработку.
func (b *Buffer) AddFacts(facts []Fact) {
	b.mu.Lock()
	b.queue = append(b.queue, facts...) // Добавляем факты в очередь
	b.mu.Unlock()

	b.wg.Add(len(facts)) // Увеличиваем счетчик wg на количество фактов
	for _, fact := range facts {
		go b.process(fact) // Запускаем обработку факта в отдельной горутине
	}
}

// process обрабатывает один факт.
func (b *Buffer) process(fact Fact) {
	defer b.wg.Done() // Уменьшаем счетчик wg, когда обработка завершена
	if err := sendFact(fact); err != nil {
		log.Printf("Ошибка отправки: %v", err)
	} else {
		log.Printf("Факт %d успешно отправлен", fact.Value)
	}
}

// sendFact выполняет отправку с повторами при ошибках.
func sendFact(fact Fact) error {
	const maxRetries = 3
	const retryDelay = 1 * time.Second

	var err error
	for i := 0; i < maxRetries; i++ {
		if err = trySend(fact); err == nil {
			return nil
		}
		log.Printf("Попытка %d для факта %d: %v", i+1, fact.Value, err)
		time.Sleep(retryDelay)
	}
	return fmt.Errorf("сбой после %d попыток: %v", maxRetries, err)
}

// trySend выполняет один HTTP POST запрос.
func trySend(fact Fact) error {
	form := url.Values{}
	form.Set("period_start", fact.PeriodStart)
	form.Set("period_end", fact.PeriodEnd)
	form.Set("period_key", fact.PeriodKey)
	form.Set("indicator_to_mo_id", strconv.Itoa(fact.IndicatorToMoID))
	form.Set("indicator_to_mo_fact_id", "0")
	form.Set("value", strconv.Itoa(fact.Value))
	form.Set("fact_time", fact.FactTime)
	form.Set("is_plan", map[bool]string{true: "1", false: "0"}[fact.IsPlan])
	form.Set("auth_user_id", strconv.Itoa(fact.AuthUserID))
	form.Set("comment", fact.Comment)

	req, err := http.NewRequest("POST", "https://development.kpi-drive.ru/_api/facts/save_fact", strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %v", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Authorization", "Bearer 48ab34464a5573519725deb5865cc74c")

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка запроса: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("код %d: %s", resp.StatusCode, body)
	}
	return nil
}

func main() {
	log.Println("Запуск буфера...")
	startTime := time.Now()

	buffer := NewBuffer()

	facts := make([]Fact, 10)
	for i := 0; i < 10; i++ {
		facts[i] = Fact{
			PeriodStart:         "2024-12-01",
			PeriodEnd:           "2024-12-31",
			PeriodKey:           "month",
			IndicatorToMoID:     227373,
			IndicatorToMoFactID: 0,
			Value:               i + 1,
			FactTime:            "2024-12-31",
			IsPlan:              false,
			AuthUserID:          40,
			Comment:             fmt.Sprintf("buffer Last_name %d", i+1),
		}
	}

	buffer.AddFacts(facts)
	buffer.wg.Wait()

	duration := time.Since(startTime)
	log.Printf("Все факты обработаны. Общее время выполнения: %v", duration)
}
