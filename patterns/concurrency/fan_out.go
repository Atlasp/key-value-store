package concurrency

func Split(source <-chan int, n int) []<-chan int {
	destinations := make([]<-chan int, 0) // Create dest slice

	for i := 0; i < n; i++ { // Create n destination channels
		ch := make(chan int)
		destinations = append(destinations, ch)

		go func() { // Each channel gets a dedicated goroutine that competes for reads
			defer close(ch)

			for val := range source {
				ch <- val
			}
		}()
	}

	return destinations
}
