package reliability

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Circuit func(ctx context.Context) (string, error)

func Breaker(circuit Circuit, failureThreshold uint) Circuit {
	var consecutiveFailure = 0
	var lastAttempt = time.Now()
	var m sync.RWMutex

	return func(ctx context.Context) (string, error) {
		m.RLock()

		d := consecutiveFailure - int(failureThreshold)

		fmt.Println(consecutiveFailure)
		fmt.Println(lastAttempt)

		if d >= 0 {
			shouldRetryAt := lastAttempt.Add(time.Second * 2 << d)
			if !time.Now().After(shouldRetryAt) {
				m.RUnlock()
				return "", errors.New("service unreachable")
			}
		}

		m.RUnlock()

		response, err := circuit(ctx)
		m.Lock()
		defer m.Unlock()

		lastAttempt = time.Now()

		if err != nil {
			consecutiveFailure++
			return "", err
		}

		consecutiveFailure = 0

		return response, nil
	}
}
