// Даны два канала. В первый пишутся числа.
// Задача в том, чтобы числа читались из первого канала по мере поступления, что-то с ними делали (различные операции) и результат записывался во второй канал.

package main

import (
	"fmt"
)

func main() {
	naturals := make(chan int)
	squares := make(chan int)

	go func() {
		for i := 1; i < 11; i++ {
			naturals <- i
		}

		close(naturals)
	}()

	go func() {
		for v := range naturals {
			fmt.Println(v)
			sq := v * v
			squares <- sq
		}

		close(squares)
	}()

	for x := range squares {
		fmt.Println("square:", x)
	}

	fmt.Println("Done")
}
