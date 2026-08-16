package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	ch := make(chan int)
	go func() {
		defer close(ch)
		for i := 1; i < 15; i++ {
			ch <- i
			time.Sleep(100 * time.Millisecond)
		}
	}()

	n := 3
	chans := SplitChannel(ch, n)

	var wg sync.WaitGroup
	wg.Add(n)

	for chanIndx, chnl := range chans {
		go func(chnl <-chan int) {
			defer wg.Done()
			for val := range chnl {
				fmt.Println("Chan ", chanIndx, ", val ", val)
			}
		}(chnl)
	}

	wg.Wait()
}

func SplitChannel(inputCh <-chan int, n int) []chan int {
	// Длина (len): n (доступны индексы от 0 до n-1).
	// Емкость (cap): n (так как максимальный размер равен длине, если не указан третий аргумент).
	res := make([]chan int, n)
	// fmt.Println(len(res))
	// fmt.Println(cap(res))

	for i := 0; i < n; i++ {
		res[i] = make(chan int)
	}

	// Мгновенный возврат результата: Функция SplitChannel должна сразу вернуть срез каналов []chan int,
	// чтобы вызывающий код мог начать их использовать.
	// Если бы цикл чтения (for value := range inputCh) работал в основном потоке,
	// функция зависла бы на первой же строчке и никогда не вернула управление.
	go func() {
		// Закрываем каналы в конце, чтобы получатели знали об окончании данных
		defer func() {
			for _, ch := range res {
				close(ch)
			}
		}()

		idx := 0
		for val := range inputCh {
			res[idx] <- val
			// Переключаемся на следующий канал по кругу
			idx = (idx + 1) % n
		}
	}()

	return res
}
