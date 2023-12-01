package concurrency

import "sync"

func Funnel(sources ...<-chan int) <-chan int {
	dest := make(chan int) // The shared output channel

	var wg sync.WaitGroup // Used to automatically close dest when all sources are closed

	wg.Add(len(sources)) // Set the size of WaitGroup

	for _, ch := range sources { // Start a routine for each source
		go func(c <-chan int) {
			defer wg.Done() // Notify WaitGroup when c closes
			for n := range c {
				dest <- n
			}
		}(ch)
	}

	go func() {
		wg.Wait() // Start a goroutine to close dest after all sources close
		close(dest)
	}()

	return dest
}
