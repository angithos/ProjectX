import weaviate
import os
from weaviate.classes.init import Auth
from dotenv import load_dotenv

load_dotenv()  # Load .env file

# client = weaviate.Client(
#     url=os.getenv("WEAVIATE_URL"),
#     auth_client_secret=weaviate.AuthApiKey(os.getenv("WEAVIATE_API_KEY"))
# )

# Instantiate your client (not shown). e.g.:
# client = weaviate.connect_to_weaviate_cloud(...) or
# client = weaviate.connect_to_local(...)
client = weaviate.connect_to_weaviate_cloud(
    cluster_url=os.getenv("WEAVIATE_URL"),
   auth_credentials=Auth.api_key(os.getenv("WEAVIATE_API_KEY")),
)

try:
    # Work with the client here - e.g.:
    assert client.is_live()
    pass

finally:  # This will always be executed, even if an exception is raised
    client.close()