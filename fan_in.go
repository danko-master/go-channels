package main

import (
	"fmt"
	"sync"
	"time"
)

func main() {
	ch1 := make(chan int)
	// Здесь будет fatal error: all goroutines are asleep - deadlock!
	// defer close(ch1)

	ch2 := make(chan int)
	// defer close(ch2)

	ch3 := make(chan int)
	// defer close(ch3)

	go func() {
		// В варианте с единой анонимной функцией func() { close(ch1); close(ch2); close(ch3) }()
		// паника на первом же close(ch1) прервет выполнение всей функции, и ch2 с ch3 не закроются.
		// defer func() {
		// 	close(ch1)
		// 	close(ch2)
		// 	close(ch3)
		// }()

		// В Go отложенные вызовы (defer) выполняются по принципу LIFO (последним пришел — первым вышел) и продолжают работать,
		// даже если в функции произошла паника.
		defer close(ch1)
		defer close(ch2)
		defer close(ch3)

		for i := 0; i < 5; i++ {
			ch1 <- i
			ch2 <- i * 10
			ch3 <- i * 100
			time.Sleep(1 * time.Second)
		}
	}()

	for value := range MergeChannels(ch1, ch2, ch3) {
		fmt.Println(value)
	}
}

func MergeChannels(channels ...<-chan int) <-chan int {
	var wg sync.WaitGroup
	chRes := make(chan int)

	wg.Add(len(channels))
	for _, chnl := range channels {
		go func(ch <-chan int) {
			defer wg.Done()
			for val := range ch {
				chRes <- val
			}
		}(chnl)
	}

	go func() {
		wg.Wait()
		close(chRes)
	}()

	return chRes
}
