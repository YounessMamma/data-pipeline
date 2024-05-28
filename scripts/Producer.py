from confluent_kafka import Producer
import random
import string
import time

# Kafka producer configuration
bootstrap_servers = 'kafka:9092'
topic_name = 'test_topic'

# Create Kafka producer
producer = Producer({'bootstrap.servers': bootstrap_servers})

# List of transaction description templates
description_templates = [
    'VIR SEPA RECU /DE {name} /MOTIF SALAIRE MENSUEL {name} /REF {ref}',
    'VIR SEPA RECU /DE {name} /MOTIF REMUNERATION {name} /REF {ref}',
    'VIR SEPA RECU /DE {name} /MOTIF PAYEMENT SALAIRE {name} /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF SALAIRE MENSUEL /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF SALAIRE DU MOIS /REF {ref}',
    'VIR SEPA RECU /DE CAISSE DE RETRAITE {name} /MOTIF PENSION RETRAITE /REF {ref}',
    'VIR RECU /DE PENSION {name} /MOTIF PENSION RETRAITE /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF REMBOURSMENT DE PREMIUM /REF {ref}',
    'VIR SEPA RECU /DE {name} /MOTIF REMBOURSEMENT APRES VENTE /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF RMBRSMNT APRES RETOUR {product} /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF REMBOURSEMENT SURPLUS FRAIS /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF REMBOURSEMENT SURPLUS PAIEMENT /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF REMBOURSEMENT TRIMESTRIEL /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF REMBOURSEMENT ACHAT RETURN /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF REFD SURPLUS FRAIS /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF RMBRSMNT ASSURANCE VOYAGE /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF RMBSEMENT SURPLUS CHARGE /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF ALLOCATIONS FAMILIALES /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF BOURSE MUNICIPALE /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF BOURSE D\'ETUDE /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF ALLOCATIONS TRANSPORT D\'ELEVE /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF RSA /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF REMBOURSEMENT FRAIS DENTAIRES /REF {ref}',
    'VIR SEPA RECU /DE {company} /MOTIF REMBOURSEMENT FRAIS {ref}'
]

def generate_random_transaction():
    # Choose a random description template
    template = random.choice(description_templates)
    
    # Fill in placeholders with random data
    transaction = {
        'name': ''.join(random.choices(string.ascii_uppercase, k=5)),
        'company': ''.join(random.choices(string.ascii_uppercase, k=8)),
        'ref': ''.join(random.choices(string.digits, k=10)),
        'product': ''.join(random.choices(string.ascii_uppercase, k=6))  # Add 'product' key
    }
    
    # Format transaction description
    transaction_description = template.format(**transaction)
    
    return transaction_description

# Start time
start_time = time.time()

# Run for one minute
while True:
    # Generate a random transaction
    transaction = generate_random_transaction()
    
    # Send message to Kafka
    producer.produce(topic_name, value=transaction)
    
    # Sleep for a short interval to control the message sending rate
    time.sleep(0.1)

# Wait for all messages to be delivered
producer.flush()
