=Producer=
python producer.py --kafka Broker0,broker1,broker2 --topic MyAwesomeTopic

=Consumer=
python consumer.py --kafka Broker0,broker1,broker2 --topic MyAwesomeTopic --group MyconsumerGroup

If you don't set "--group" it defaults to "kafka-python".


