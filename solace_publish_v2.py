import xml.etree.ElementTree as ET
from solace.messaging import MessagingService
from solace.messaging.resources.topic import Topic
from solace.messaging.publisher.persistent_message_publisher import PersistentMessagePublisherBuilder
from solace.messaging.config.solace_properties import TransportProperties, AuthenticationProperties
from solace.messaging.config.service import ServiceProperties
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError

# Configuration for Solace broker
HOST = "YOUR_SOLACE_HOST"
VPN_NAME = "YOUR_VPN"
SOLACE_USERNAME = "YOUR_USERNAME"
SOLACE_PASSWORD = "YOUR_PASSWORD"
QUEUE_NAME = "YOUR_QUEUE_NAME"

# Create an XML object
root = ET.Element("Library")
book = ET.SubElement(root, "Book")
book.set("id", "1")
title = ET.SubElement(book, "Title")
title.text = "The Great Gatsby"
author = ET.SubElement(book, "Author")
author.text = "F. Scott Fitzgerald"

# Convert XML object to a string
xml_string = ET.tostring(root, encoding='unicode')

# Configure the messaging service
service = MessagingService.builder() \
    .from_properties({
        TransportProperties.HOST: HOST,
        AuthenticationProperties.SCHEME_BASIC_USER_NAME: SOLACE_USERNAME,
        AuthenticationProperties.SCHEME_BASIC_PASSWORD: SOLACE_PASSWORD,
        ServiceProperties.VPN_NAME: VPN_NAME
    }) \
    .build()

# Connect to the messaging service
try:
    service.connect()
except PubSubPlusClientError as e:
    print(f"Failed to connect to the messaging service: {e}")
    exit(1)

# Create a direct message publisher and start it
publisher = service.create_direct_message_publisher_builder().build()
publisher.start()

# Define the queue as a topic object
queue_topic = Topic.of(QUEUE_NAME)

# Publish the XML message
try:
    message = service.message_builder().build(xml_string)
    publisher.publish(destination=queue_topic, message=message)
    print("XML message published successfully.")
except Exception as e:
    print(f"An error occurred while publishing the message: {e}")

# Disconnect the publisher and messaging service
publisher.terminate()
service.disconnect()
