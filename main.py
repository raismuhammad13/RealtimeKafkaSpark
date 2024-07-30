import psycopg2
import requests
import random
from confluent_kafka import SerializingProducer
import simplejson as json  


BASE_URL = 'https://randomuser.me/api/?nat=gb'
PARTIES = ['Management_Party', "Savior Party", "Tech Republic Parth"]

random.seed(21)

def create_tables(conn, cur):
    cur.execute("""
             CREATE TABLE IF NOT EXISTS candidates (
                candidate_id VARCHAR(255) PRIMARY KEY,
                candidate_name VARCHAR(255),
                party_affiliation VARCHAR(255),
                biography TEXT,
                campaign_platform TEXT,
                photo_url TEXT
                )     
            """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS voters (
                voter_id VARCHAR(255) PRIMARY KEY,
                voter_name VARCHAR(255),
                date_of_birth DATE,
                gender VARCHAR(255),
                nationality VARCHAR(255),
                registration_number VARCHAR(255),
                address_street VARCHAR(255),
                address_city VARCHAR(255),
                address_state VARCHAR(255),
                address_country VARCHAR(255),
                address_postcode VARCHAR(255),
                email VARCHAR(255),
                phone_number VARCHAR(255),
                picture TEXT,
                registred_age INTEGER
                )
            """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS votes (
                voter_id VARCHAR(255) UNIQUE,
                candidate_id VARCHAR(255),
                voting_time TIMESTAMP,
                vote int DEFAULT 1,
                primary key (voter_id, candidate_id)
                )
            """)
    conn.commit()

def generate_candidate(candidate_number, total_parties):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number%2==1 else 'male'))
    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            'candidate_id': user_data['login']['uuid'],
            'candidate_name': f"{user_data['name']['first']} {user_data['name']['last']}",
            'party_affiliation': PARTIES[candidate_number % total_parties],
            'biography': "A brief biography of the candidate",
            'campaign_platform': "Key compaign promisses and or platform",
            'photo_url': user_data['picture']['large']
        }
    else:
        print("Error fetching the candidate data.")

def generate_voter_data():
    response = requests.get(BASE_URL)
    if response.status_code == 200:
        user_data = response.json()['results'][0]

        return {
            "voter_id": user_data['login']['uuid'],
            "voter_name": f"{user_data['name']['first']} {user_data['name']['last']}",
            "date_of_birth": user_data['dob']['date'],
            "gender": user_data['gender'],
            "nationality": user_data['nat'],
            "registration": user_data['login']['username'],
            "address": {
                "street": f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
                "city": user_data['location']['city'],
                "state": user_data['location']['state'],
                "country": user_data['location']['country'],
                "postcode": user_data['location']['postcode']
            },
            "email": user_data['email'],
            "phone_number": user_data['phone'],
            "picture": user_data['picture']['large'],
            "registered_age": user_data['registered']['age']
        } 
    else:
        print("errror is occure while api calling.")   

def insert_voters(conn, cur, voter):
    cur.execute("""
                INSERT INTO voters (voter_id, voter_name, date_of_birth, gender, nationality, registration_number
                , address_street, address_city, address_state, address_country, address_postcode
                , email, phone_number, picture, registred_age)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        voter['voter_id'], voter['voter_name'], voter['date_of_birth'], voter['gender'], voter['nationality']
                        , voter['registration'], voter['address']['street'], voter['address']['city'], voter['address']['state']
                        , voter['address']['country'], voter['address']['postcode'], voter['email'], voter['phone_number']
                        , voter['picture'], voter['registered_age']
                    ))
    conn.commit()
    print("Voters data inserted, Successfully......")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to topic {msg.topic()} [{msg.partition()}]")

if __name__ == "__main__":
    # Creating the kafka producer
    producer = SerializingProducer({'bootstrap.servers': 'localhost:9092'})
    try:
        conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
        cur = conn.cursor()

        # Creating the tables
        create_tables(conn, cur)

        # Selecting all the candidates from the candidates table.
        cur.execute("""
            SELECT * FROM candidates
            """)

        # Getting the candidates
        candidates = cur.fetchall()
        # print(candidates)

        if len(candidates) == 0:
            for i in range(3):
                candidate = generate_candidate(i, 3)
                print(candidate)
                cur.execute("""
                            INSERT INTO candidates(candidate_id, candidate_name, party_affiliation, biography, campaign_platform, photo_url)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """,
                                (
                                    candidate['candidate_id'], candidate['candidate_name'], candidate['party_affiliation'], 
                                    candidate['biography'], candidate['campaign_platform'], candidate['photo_url']
                                ))
                conn.commit()
                print("Candidates data Inserted, Successfully......")

        # Getting the voters data
        for i in range(1000):
            voter_data = generate_voter_data()
            # Inserting the voters data
            insert_voters(conn, cur, voter_data)
           
            # Producing the data into kafka topic
            producer.produce(
                "voters_topic",
                key=voter_data['voter_id'],
                value=json.dumps(voter_data),
                on_delivery=delivery_report
            )

            print("Produced voter {}, data: {}".format(i, voter_data))
            
    except Exception as e:
        print("error occured: ", e)