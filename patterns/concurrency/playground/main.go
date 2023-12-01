package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud_native/patterns/concurrency"
)

func main() {
	//tryoutFanIn()
	//tryoutFanOut()
	//tryoutFuture()
	//tryoutSharding()
}

func tryoutSharding() {
	shardedMap := concurrency.NewShardMap(5)
	shardedMap.Set("alpha", 1)
	shardedMap.Set("beta", 2)
	shardedMap.Set("gamma", 3)
	fmt.Println(shardedMap.Get("alpha"))
	fmt.Println(shardedMap.Get("beta"))
	fmt.Println(shardedMap.Get("gamma"))
	keys := shardedMap.Keys()
	for _, k := range keys {
		fmt.Println(k)
	}

}

func tryoutFuture() {
	future := concurrency.SlowFunction(context.Background())

	res, err := future.Result()
	if err != nil {
		fmt.Println("error: ", err)
		return
	}

	fmt.Println(res)
}

func tryoutFanOut() {
	source := make(chan int)              // The input channel
	dests := concurrency.Split(source, 5) // Retrieve 5 output channels

	go func() { // Send number 1..10 to source and close it when we're done
		for i := 0; i <= 10; i++ {
			source <- i
		}

		close(source)
	}()

	var wg sync.WaitGroup // Use WaitGroup to wait until the output channels all close
	wg.Add(len(dests))

	for i, ch := range dests {
		go func(i int, d <-chan int) {
			defer wg.Done()

			for val := range d {
				fmt.Printf("#%d got %d \n", i, val)
			}
		}(i, ch)
	}

	wg.Wait()
}

func tryoutFanIn() {
	sources := make([]<-chan int, 0)

	for i := 0; i < 3; i++ {
		ch := make(chan int)

		sources = append(sources, ch)

		go func() {
			defer close(ch)

			for j := 1; j <= 5; j++ {
				ch <- j
				time.Sleep(time.Second)
			}
		}()
	}

	dest := concurrency.Funnel(sources...)

	for d := range dest {
		fmt.Println(d)
	}
}
