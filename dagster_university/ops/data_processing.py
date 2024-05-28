from dagster import op, In, Out  # type: ignore
from huggingface_hub import login  # type: ignore
from setfit import SetFitModel  # type: ignore
from confluent_kafka import Consumer, KafkaError, KafkaException # type: ignore

@op(ins={"s3_data": In()}, out={"result": Out()})
def inferencing_model(context, s3_data):
    kafka_consumer_config = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'my_consumer_group',
        'auto.offset.reset': 'earliest'
    }
    
    kafka_consumer = Consumer(kafka_consumer_config)
    batch_size = 10  # Adjust the batch size as needed
    batch_data = []
    
    try:
        kafka_consumer.subscribe(['transformed_data'])

        while True:
            msg = kafka_consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    context.log.warning('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                transformed_data = msg.value().decode('utf-8')  # Assuming UTF-8 encoding
                data = transformed_data.split('\t')
                
                batch_data.append(data)
                
                if len(batch_data) >= batch_size:
                    try:
                        context.log.info(f"Batch data: {batch_data}")

                        login('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')
                        model = SetFitModel.from_pretrained("EffyisDATALAB/setfit_trx_classification_OrdalieTech_Solon-embeddings-large-0.1_particular_91.67_CRDT")
                        
                        # Assuming 'batch_data' is the input format expected by your model
                        results = model.predict(batch_data)

                        context.log.info(f"Inference results: {results}")
                        
                        # Return the results
                        return results
                        
                    except Exception as e:
                        context.log.error(f"Inference failed: {e}")
                    finally:
                        # Clear the batch data after processing
                        batch_data = []

    except Exception as ex:
        context.log.error(f"An error occurred: {ex}")
    finally:
        # Optionally log when the consumer is closing
        context.log.info("Closing Kafka consumer")
        kafka_consumer.close()
