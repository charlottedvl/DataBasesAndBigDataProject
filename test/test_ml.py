from bertopic import BERTopic
from sklearn.datasets import fetch_20newsgroups


mystr = "- We&#x27;re an AI startup democratizing access to government contracts by making it easier for small businesses to find and respond to soul-crushingly complex RFPs and paperwork.We just raised $3m (April 2024) and have strong GTM traction.We&#x27;re looking for a founding engineer to join our 4-person team. Long hours at the keyboard, bitter cold, long months of complete darkness, constant danger, safe return unknown. Honor, recognition, and enormous riches in case of success.Full Job Posting: https:&#x2F;&#x2F;wellfound.com&#x2F;l&#x2F;2A9rLBCurrent Tech Stack: Python&#x2F;Django | JavaScript&#x2F;Vue.js (React experience is acceptable too!) | Postgres&#x2F;Relational DBIf you’re a hungry, talented full-stack engineer that thrives in small companies, wants to solve interesting problems with the latest AI tools, and views a culture of hard work as a feature, not a bug, shoot us a note! Team AT breezeRFP.com"
docs = fetch_20newsgroups(subset='all',  remove=('headers', 'footers', 'quotes'))['data']
print(docs)
topic_model = BERTopic()
topic_model.fit_transform(docs)
