package tasktrek

import (
	"context"
	"errors"
	"fmt"

	"github.com/Azure/go-amqp"
)

type tasktrek struct {
	conn       *amqp.Conn
	session    *amqp.Session
	receiver   *amqp.Receiver
	sender     *amqp.Sender
	taskFunc   TaskFunc
	resultChan chan interface{}
}

type TaskMsg struct {
	*amqp.Message
}

type TaskFunc func(context.Context, TaskMsg, chan interface{}) error

// NewService creates a new service where we create the connection to the broker and prepare
// our service with broker sender and receiver. This function returns a service object to perform
// further functions and a close function which when called can stop the execution of the process.
func NewService(ctx context.Context, c, recCh string) (*tasktrek, func(*tasktrek), error) {

	//create connection
	conn, err := amqp.Dial(ctx, c, nil)
	if err != nil {
		return nil, nil, err
	}

	// create new session
	session, err := conn.NewSession(context.TODO(), nil)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	//create a new receiver
	recevier, err := session.NewReceiver(ctx, recCh, nil)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	//create a new sender on the same topic
	sender, err := session.NewSender(ctx, recCh, nil)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	service := tasktrek{
		conn:       conn,
		session:    session,
		receiver:   recevier,
		sender:     sender,
		resultChan: make(chan interface{}),
	}

	closeFn := func(service *tasktrek) {
		service.conn.Close()
		close(service.resultChan)
	}

	return &service, closeFn, err

}

// RegisterTaskFunc will register the function which would be executed on all the messages received on the
// channel specified. This function will receive the passed context to StartProcessing(), the message
// received on the message broker topic and the channel in which the function can pass in the result of each execution
// This will be received by iterating the channel received from AwaitProcessing()
func (t *tasktrek) RegisterTaskFunc(fn TaskFunc) error {
	if fn == nil {
		return errors.New("task function can not be empty")
	}

	t.taskFunc = fn
	return nil
}

// StartProcessing will start the message processing. This processes the message
// in a new goroutine so will not be blocking the code for the processing. See AwaitProcessing()
// on how to block code for process completion. The function would close the connection and exit
// the processing if the passed context is cancelled.
func (t *tasktrek) StartProcessing(ctx context.Context) {
	fmt.Println("Starting message ingestion")
	go func() {
		for {
			select {
			case <-ctx.Done():
				fmt.Println("context has been cancelled")
				t.conn.Close()
				close(t.resultChan)
				return
			default:
				// we receive the message
				msg, err := t.receiver.Receive(ctx, nil)
				if err != nil {
					continue
				}

				// if we encounter an error we send msg back on the channel.
				// this way the message will not be blocking the queue
				if err := t.taskFunc(ctx, TaskMsg{Message: msg}, t.resultChan); err != nil {
					fmt.Println("Error occured. Sending msg for reprocessing")
					t.sender.Send(ctx, msg, nil)
				}
				// accept the message so that it is not retried on any other instance
				t.receiver.AcceptMessage(ctx, msg)
			}

		}
	}()

}

// AwaitProcessing returns channel from which we can fetch the processing
// results. Iterate over this channel to fetch all the results and block your
// code until all the processing is done. Example code
//
//	for data := range serv.AwaitProcessing(ctx) {
//			fmt.Println(data)
//	}
func (t *tasktrek) AwaitProcessing(ctx context.Context) chan interface{} {
	return t.resultChan
}
