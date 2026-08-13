// Вам нужно написать функцию ProcessTasks, которая принимает на вход слайс функций (задач) и должна выполнить их параллельно.

package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrTimeout возвращается, если общее время выполнения превысило лимит
var ErrTimeout = errors.New("overall timeout exceed")

// Task представляет собой функцию-задачу
type Task func()

func main() {
	// demo tasks
	tasks := make([]Task, 10)
	for i := 0; i < 10; i++ {
		taskID := i
		tasks[i] = func() {
			fmt.Printf("  -> [Задача №%d]: Начало выполнения...\n", taskID)
			time.Sleep(500 * time.Millisecond) // Каждая задача эмулирует работу в 500мс
			fmt.Printf("  <- [Задача №%d]: Успешно завершена!\n", taskID)
		}
	}

	// settings
	// - Одновременно могут работать не более 3 задач.
	concurrencyLimit := 3
	// - Общий лимит времени на всё приложение — 1 секунда.
	// (За 1 секунду пул из 3 воркеров успеет сделать 2 партии задач: 3 + 3 = 6 задач. На 7-й наступит таймаут).
	totalTimeout := 1050 * time.Millisecond

	fmt.Println("[Main]: Запуск обработки задач...")
	start := time.Now()

	err := ProcessTasks(tasks, concurrencyLimit, totalTimeout)

	duration := time.Since(start)
	fmt.Printf("\n[Main]: Функция ProcessTasks завершилась за %v\n", duration)

	if err != nil {
		fmt.Printf("[Main]: Получена ошибка: %v\n", err)
	} else {
		fmt.Println("[Main]: Все задачи успешно выполнены!")
	}

	// Даем консоли секунду, чтобы убедиться, что после выхода из функции
	// в фоне не осталось никаких «забытых» горутин, которые продолжают писать логи.
	time.Sleep(1 * time.Second)
	fmt.Println("[Main]: Программа завершена.")
}

func ProcessTasks(tasks []Task, concurencyLimit int, totalTimeout time.Duration) error {
	// Контекст с таймаутом на всю работу
	ctx, cancel := context.WithTimeout(context.Background(), totalTimeout)
	defer cancel()

	var wgTasks sync.WaitGroup
	// Канал-семафор
	sem := make(chan struct{}, concurencyLimit)

	// Флаг для отслеживания, прервались ли мы по таймауту во время обхода цикла
	var loopInterrupted bool

	for i, task := range tasks {
		// Проверяем контекст
		if ctx.Err() != nil {
			fmt.Printf("[Планировщик]: Таймаут сработал до запуска задачи №%d. Прерываем цикл.\n", i)
			loopInterrupted = true
			// break мгновенно прерывает выполнение текущего цикла и передает управление коду, который идет сразу после него
			break

			// В данной задаче нельзя вызывать return
			// return i - Мгновенно выходим из цикла и завершаем функцию
			// Но нам требуется продолжить выполнение функции
		}

		select {
		case <-ctx.Done():
			fmt.Printf("[Планировщик]: Таймаут сработал во время ожидания слота для задачи №%d.\n", i)
			loopInterrupted = true
			// break выходит только из самого внутреннего блока select или switch
			break
		case sem <- struct{}{}:
			// Заняли слот в пуле, увеличиваем счетчик
			wgTasks.Add(1)

			go func(id int, t Task) {
				// Необходимо освободить слот в семафоре
				defer func() {
					<-sem
					wgTasks.Done()
				}()
				// run task
				t()
			}(i, task)
		}

		// Если вышли по ctx.Done с флагом loopInterrupted
		if loopInterrupted {
			// Выход из цикла
			break
		}
	}

	// Сигнальный канал
	done := make(chan struct{})
	go func() {
		// Ждем всех
		wgTasks.Wait()
		// Закрываем канал
		close(done)
	}()

	// Итог
	select {
	// Это операция чтения из канала.
	// Поток выполнения (горутина) блокируется на этой строчке и ждет, пока в канал done кто-нибудь не запишет пустую структуру (done <- struct{}{})
	// или пока канал done не будет закрыт с помощью функции close(done).
	// Чтение из закрытого канала в Go всегда возвращает нулевое значение типа (в данном случае struct{}), не вызывая паники.
	case <-done:
		// Все задачи, которые мы успели запустить, успешно дошли до конца.
		// Но если мы вышли из цикла досрочно из-за таймаута, нужно вернуть ошибку.
		if ctx.Err() != nil || loopInterrupted {
			return ErrTimeout
		}
		return nil
	case <-ctx.Done():
		// Таймаут наступил, пока запущенные воркеры еще выполняли работу в фоне.
		fmt.Println("[Планировщик]: Общее время истекло! Ожидаем завершения уже запущенных задач (Graceful Shutdown)...")
		<-done // Ждем, пока запущенные горутины безопасно допишут данные/логи
		return ErrTimeout
	}
}
