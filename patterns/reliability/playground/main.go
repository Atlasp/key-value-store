package main

import (
	"context"
	"errors"
	"fmt"

	"cloud_native/patterns/reliability"
)

func main() {
	tryOutCircuitBreaker()
}

func tryOutCircuitBreaker() {
	x := reliability.Circuit(func(ctx context.Context) (string, error) {
		return "", errors.New("intentional error")
	})

	brokenCircuit := reliability.Breaker(x, 2)

	fmt.Println(brokenCircuit(context.Background()))
	fmt.Println(brokenCircuit(context.Background()))
	fmt.Println(brokenCircuit(context.Background()))
}
