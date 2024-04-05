package amqp

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/argoproj/argo-events/common"
	"github.com/argoproj/argo-events/common/logging"
	apicommon "github.com/argoproj/argo-events/pkg/apis/common"
	"github.com/argoproj/argo-events/pkg/apis/sensor/v1alpha1"
	"github.com/argoproj/argo-events/sensors/triggers"
	amqplib "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"
	"time"
)

type AMQPTrigger struct {
	// Sensor object
	Sensor *v1alpha1.Sensor
	// Trigger reference
	Trigger *v1alpha1.Trigger
	// Logger to log stuff
	Logger *zap.SugaredLogger
	// Conn refers to the RabbitMQ client connection
	Conn *amqplib.Connection
}

func NewAMQPTrigger(sensor *v1alpha1.Sensor, trigger *v1alpha1.Trigger, amqpConnections common.StringKeyedMap[*amqplib.Connection], logger *zap.SugaredLogger) (*AMQPTrigger, error) {
	amqptrigger := trigger.Template.AMQP

	conn, ok := amqpConnections.Load(trigger.Template.Name)
	if !ok {
		err := common.DoWithRetry(amqptrigger.ConnectionBackoff, func() error {
			c := amqplib.Config{
				Heartbeat: 10 * time.Second,
				Locale:    "en_US",
			}
			if amqptrigger.TLS != nil {
				tlsConfig, err := common.GetTLSConfig(amqptrigger.TLS)
				if err != nil {
					return err
				}
				c.TLSClientConfig = tlsConfig
			}
			if amqptrigger.Auth != nil {
				username, err := common.GetSecretFromVolume(amqptrigger.Auth.Username)
				if err != nil {
					return fmt.Errorf("username not found, %w", err)
				}
				password, err := common.GetSecretFromVolume(amqptrigger.Auth.Password)
				if err != nil {
					return fmt.Errorf("password not found, %w", err)
				}
				c.SASL = []amqplib.Authentication{&amqplib.PlainAuth{
					Username: username,
					Password: password,
				}}
			}

			var err error
			var url string
			if amqptrigger.URLSecret != nil {
				url, err = common.GetSecretFromVolume(amqptrigger.URLSecret)
				if err != nil {
					return fmt.Errorf("urlSecret not found: %w", err)
				}
			} else {
				url = amqptrigger.URL
			}
			logger.Info("trying to dial")
			conn, err = amqplib.DialConfig(url, c)
			if err != nil {
				return err
			}

			err = declareExchange(logger, conn, amqptrigger)
			if err != nil {
				return err
			}

			amqpConnections.Store(trigger.Template.Name, conn)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("failed to connect to amqp broker, %w", err)
		}
	}

	return &AMQPTrigger{
		Sensor:  sensor,
		Trigger: trigger,
		Logger:  logger.With(logging.LabelTriggerType, apicommon.AMQPTrigger),
		Conn:    conn,
	}, nil

}

func (t AMQPTrigger) GetTriggerType() apicommon.TriggerType {
	return apicommon.AMQPTrigger
}

func (t AMQPTrigger) FetchResource(ctx context.Context) (interface{}, error) {
	return t.Trigger.Template.AMQP, nil
}

func (t AMQPTrigger) ApplyResourceParameters(events map[string]*v1alpha1.Event, resource interface{}) (interface{}, error) {
	fetchedResource, ok := resource.(*v1alpha1.AMQPTrigger)
	if !ok {
		return nil, fmt.Errorf("failed to interpret the fetched trigger resource")
	}

	resourceBytes, err := json.Marshal(fetchedResource)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal the amqp trigger resource, %w", err)
	}
	parameters := fetchedResource.Parameters
	if parameters != nil {
		updatedResourceBytes, err := triggers.ApplyParams(resourceBytes, parameters, events)
		if err != nil {
			return nil, err
		}
		var t *v1alpha1.AMQPTrigger
		if err := json.Unmarshal(updatedResourceBytes, &t); err != nil {
			return nil, fmt.Errorf("failed to unmarshal the updated amqp trigger resource after applying resource parameters, %w", err)
		}
		return t, nil
	}
	return resource, nil
}

func (t AMQPTrigger) Execute(ctx context.Context, events map[string]*v1alpha1.Event, resource interface{}) (interface{}, error) {
	trigger, ok := resource.(*v1alpha1.AMQPTrigger)
	if !ok {
		return nil, fmt.Errorf("failed to interpret the trigger resource")
	}
	if trigger.Payload == nil {
		return nil, fmt.Errorf("payload parameters are not specified")
	}
	payload, err := triggers.ConstructPayload(events, trigger.Payload)
	if err != nil {
		return nil, err
	}

	channel, err := t.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel, %w", err)
	}

	err = channel.PublishWithContext(ctx, trigger.ExchangeName, trigger.RoutingKey, false, false,
		amqplib.Publishing{
			Body: payload,
		})
	if err != nil {
		return nil, fmt.Errorf("failed to publish message on exchange %s with routing key %s, %w",
			trigger.ExchangeName, trigger.RoutingKey, err)
	}

	t.Logger.Infow("successfully publish new message", zap.String("exchangeName", trigger.ExchangeType),
		zap.String("routingKey", trigger.RoutingKey))

	return nil, nil
}

func (t AMQPTrigger) ApplyPolicy(ctx context.Context, resource interface{}) error {
	return nil
}

func declareExchange(logger *zap.SugaredLogger, conn *amqplib.Connection, amqptrigger *v1alpha1.AMQPTrigger) error {
	channel, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open channel, %w", err)
	}

	if amqptrigger.ExchangeDeclare == nil {
		logger.Infow("no exchange config found, declare exchange %s with default values", amqptrigger.ExchangeName)
		amqptrigger.ExchangeDeclare = &v1alpha1.AMQPExchangeDeclareConfig{
			Durable:    true,
			AutoDelete: false,
			Internal:   false,
			NoWait:     false,
		}
	}

	err = channel.ExchangeDeclare(amqptrigger.ExchangeName,
		amqptrigger.ExchangeType,
		amqptrigger.ExchangeDeclare.Durable,
		amqptrigger.ExchangeDeclare.AutoDelete,
		amqptrigger.ExchangeDeclare.Internal,
		amqptrigger.ExchangeDeclare.NoWait,
		nil)
	if err != nil {
		return fmt.Errorf("failed to declare exchage with name %s and type %s, %w",
			amqptrigger.ExchangeName, amqptrigger.ExchangeType, err)
	}
	return nil
}
