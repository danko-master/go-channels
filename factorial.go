// An array of numbers.
// For each number it is necessary to calc the factorial
// Calc can be heavy, they need to be performed in parallel

// Нам поступает массив чисел, для каждого из которых нужно вычислить факториал.
// Так как вычисления могут быть тяжелыми, их нужно выполнять параллельно.

package main

import (
	"fmt"
	"sync"
)

func main() {
	nums := []int{2, 4, 5, 6, 7, 1, 0}
	numWorkers := 3

	var wg sync.WaitGroup

	// Каналы с буфером, чтобы горутины не блокировались при отправке
	jobs := make(chan int, len(nums))
	results := make(chan int, len(nums))

	// 1. Запускаем пул воркеров
	wg.Add(numWorkers)

	for w := 1; w <= numWorkers; w++ {
		go worker(w, &wg, jobs, results)
	}

	// 2. Отправляем задачи в канал jobs
	for _, num := range nums {
		jobs <- num
	}
	// Закрываем канал, выходим из цикла range jobs
	close(jobs)

	// 3. Запускаем горутину для ожидания воркеров и закрытия каналов результатов.
	// Если делать wg.Wait() в main случится deadlock, т.к. main заблокируется
	go func() {
		wg.Wait()
		close(results)
	}()

	// 4. Читаем результат из канала
	for res := range results {
		fmt.Printf("Получен результат: %d\n", res)
	}

	// Без воркеров
	//	for i, v := range nums {
	//		fmt.Println(i)
	//		fmt.Println(v)
	//		go factorial(v)
	//		fmt.Println("---")
	//	}
}

func worker(w int, wg *sync.WaitGroup, jobs <-chan int, results chan<- int) {
	defer wg.Done()

	for num := range jobs {
		fmt.Printf("[Воркер %d] Начал вычисление для: %d\n", w, num)
		results <- factorial(num)
	}
}

// Calc factorial
func factorial(num int) int {
	if num == 0 {
		return 1
	}

	fact := 1
	for i := 1; i <= num; i++ {
		fact = i * fact
	}

	fmt.Println(fact)

	return fact
}
