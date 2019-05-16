package input

type Input interface {
	Run()
	Stop()
	Wait()
}

