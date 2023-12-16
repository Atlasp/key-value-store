package reliability

import "context"

type SlowFunction func(string) (string, error)

type WithContext func(context.Context, string) (string, error)

func Timeout(f SlowFunction) WithContext {
	return func(ctx context.Context, arg string) (string, error) {
		chRes := make(chan string)
		chErr := make(chan error)

		go func() {
			res, err := f(arg)
			chRes <- res
			chErr <- err
		}()

		select {
		case res := <-chRes:
			return res, <-chErr
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
}
