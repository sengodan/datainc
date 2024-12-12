import xml.etree.ElementTree as ET
from solace.messaging import MessagingService, QueueDescriptor, QueueProperties
from solace.messaging.resources.queue import Queue
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener

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

# Initialize messaging service
service = MessagingService.builder().from_properties({
    "solace.messaging.transport.host": HOST,
    "solace.messaging.service.vpn-name": VPN_NAME,
    "solace.messaging.authentication.scheme.basic.username": SOLACE_USERNAME,
    "solace.messaging.authentication.scheme.basic.password": SOLACE_PASSWORD
}).build()

# Connect to the messaging service
service.connect()

# Define the queue and its properties
queue = Queue.builder().with_name(QUEUE_NAME).with_property(QueueProperties.ACCESS_TYPE, QueueProperties.AccessType.EXCLUSIVE).build()
queue_descriptor = QueueDescriptor.of(queue)

# Create a direct message publisher and start it
publisher = service.create_direct_message_publisher_builder().build()
publisher.start()

# Define a failure listener to handle publish failures
class MyPublishFailureListener(PublishFailureListener):
    def on_failed_publish(self, message, error):
        print(f"Failed to publish message: {message.get_payload_as_string()}. Error: {error}")

publisher.set_publish_failure_listener(MyPublishFailureListener())

# Publish the XML message
try:
    message = service.message_builder().build(xml_string)
    publisher.publish(destination=queue_descriptor, message=message)
    print("XML message published successfully.")
except Exception as e:
    print(f"An error occurred: {e}")

# Disconnect the publisher and messaging service
publisher.terminate()
service.disconnect()
