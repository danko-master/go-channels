package main

import (
	"fmt"
	"sync"
)

func main() {

	a := make(chan int)
	b := make(chan int)
	c := make(chan int)

	go func() {
		for _, num := range []int{1, 2, 3} {
			a <- num
		}
		close(a)
	}()
	go func() {
		for _, num := range []int{40, 50, 60} {
			b <- num
		}
		close(b)
	}()
	go func() {
		for _, num := range []int{700, 800, 900} {
			c <- num
		}
		close(c)
	}()

	for num := range joinChannels(a, b, c) {
		fmt.Println(num)
	}
}

func joinChannels(chs ...<-chan int) <-chan int {
	resChan := make(chan int)

	go func() {
		wg := &sync.WaitGroup{}
		wg.Add(len(chs))

		for _, ch := range chs {
			go func(ch <-chan int, wg *sync.WaitGroup) {
				defer wg.Done()

				for currNum := range ch {
					resChan <- currNum
				}
			}(ch, wg)
		}

		wg.Wait()
		close(resChan)
	}()

	return resChan
}
