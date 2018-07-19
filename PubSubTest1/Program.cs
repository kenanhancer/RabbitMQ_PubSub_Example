using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Tasks;

namespace PubSubTest1
{
    class Program
    {
        static ConcurrentBag<AsyncAutoResetEvent> cb = new ConcurrentBag<AsyncAutoResetEvent>();

        static string queueName = "Test_Queue";

        static void Main(string[] args)
        {
            Parallel.For(0, 10, i => Publisher());

            Parallel.For(0, 10, i => Consumer());

            Console.ReadLine();

            foreach (var item in cb)
            {
                item.Set();
            }
        }

        static Task Publisher()
        {
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            string message = "Hello World";

            using (var connection = connectionFactory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    channel.QueueDeclare(queueName, false, false, false, null);

                    channel.BasicPublish("", queueName, null, Encoding.UTF8.GetBytes(message));

                    Console.WriteLine($"Sended message \"{message}\" in {queueName} Queue");
                }
            }

            return Task.CompletedTask;
        }

        static async Task Consumer()
        {
            ConnectionFactory connectionFactory = new ConnectionFactory
            {
                HostName = "localhost",
                UserName = "guest",
                Password = "guest"
            };

            AsyncAutoResetEvent are = new AsyncAutoResetEvent();

            cb.Add(are);

            using (var connection = connectionFactory.CreateConnection())
            {
                using (var channel = connection.CreateModel())
                {
                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body;

                        var message = Encoding.UTF8.GetString(body);

                        Console.WriteLine($"Comming message \"{message}\" from {queueName}");
                    };

                    channel.BasicConsume(queueName, true, consumer);

                    await are.WaitAsync().ConfigureAwait(false);
                }
            }
        }
    }
}
