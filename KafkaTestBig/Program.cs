using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaTestBig {
  internal class Program {
    public const string Kafka = "192.168.1.51:9092";
    public const string Topic = "topic-20m";

    private static async Task Main(string[] args) {
      Console.WriteLine("输入\n\t数字: 按数字发送相应的字节数消息；\n\tquit: 退出");
      var cts = new CancellationTokenSource();

      _ = Task.Run(() => {
        ConsumeKafka(cts.Token);
      }, cts.Token);

      //await SendToKafka(100, Topic, cts.Token);

      do {
        var input = Console.ReadLine();
        if (input != null && input.ToLower() == "quit") {
          cts.Cancel();
          break;
        }

        if (int.TryParse(input, out var messageSize)) {
          await SendToKafka(messageSize, Topic, cts.Token);
        }
      } while (true);
    }

    /// <summary>
    ///   发送 Kafka消息
    /// </summary>
    /// <param name="messageSize">消息内容</param>
    /// <param name="topic">Kafka topic</param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    public static async Task SendToKafka(int messageSize, string topic, CancellationToken cancellationToken) {
      var message = "".PadRight(messageSize, 'T');
      var conf = new ProducerConfig {
        BootstrapServers = Kafka, MessageTimeoutMs = 60000, MessageMaxBytes = 20 * 1000 * 1000
      };
      try {
        using var p = new ProducerBuilder<Null, string>(conf).Build();
        var ret = await p.ProduceAsync(topic,
          new Message<Null, string> { Value = message }, cancellationToken);
        Console.WriteLine($"发送成功，结果信息,Offset= {ret.Offset}");
      }
      catch (Exception ex) {
        Console.WriteLine($"发送消息出错。:{ex.Message}");
      }
    }

    public static Task ConsumeKafka(CancellationToken cancellationToken) {
      var conf = new ConsumerConfig {
        GroupId = "test-consumer2",
        BootstrapServers = Kafka,
        AutoCommitIntervalMs = 1000,
        EnableAutoCommit = true,
        SessionTimeoutMs = 6000,
        //MessageMaxBytes = 20 * 1000 * 1000,
        //FetchMaxBytes = 20 * 1024 * 1024,
        //FetchWaitMaxMs = 50000,
        //MaxPartitionFetchBytes = 21 * 1000 * 1000,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnablePartitionEof = true
      };
      using var consumer = new ConsumerBuilder<Ignore, string>(conf).Build();
      try {
        consumer.Subscribe(Topic);
        while (true) {
          try {
            var cr = consumer.Consume(cancellationToken);
            if (cr.IsPartitionEOF) {
              continue;
            }

            Console.WriteLine($"获取到Kafka消息,Offset:{cr.Offset} 数据大小:@{cr.Message.Value.Length}");
          }

          catch (Exception e) {
            Console.WriteLine($"获取消息出错:{e.Message}");
          }

          if (cancellationToken.IsCancellationRequested) {
            Console.WriteLine("task IsCancellationRequested.");
            break;
          }
        }
      }
      catch (Exception ex) {
        Console.WriteLine($"Subscribe closing...,{ex.Message}");
      }

      consumer.Close();
      return Task.CompletedTask;
    }
  }
}