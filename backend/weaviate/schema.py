# schema.py
import weaviate.classes.config as wc
from client import get_weaviate_client # Import the client function

def create_product_offer_schema(client):
    """
    Defines and creates the 'ProductOffer' collection schema in Weaviate.
    """
    if not client:
        print("Weaviate client not available to create schema.")
        return

    collection_name = "ProductOffer"

    try:
        # Check if the collection already exists
        if client.collections.exists(collection_name):
            print(f"Collection '{collection_name}' already exists. Deleting and recreating for fresh start.")
            client.collections.delete(collection_name)

        print(f"Creating collection '{collection_name}'...")
        client.collections.create(
            name=collection_name,
            properties=[
                wc.Property(name="name", data_type=wc.DataType.TEXT, description="Name of the product"),
                wc.Property(name="brand", data_type=wc.DataType.TEXT, description="Brand of the product"),
                wc.Property(name="description", data_type=wc.DataType.TEXT, description="Detailed description of the product"),
                # Categories is a comma-separated string in your CSV, so store as TEXT for now.
                # If you want it as an array (TEXT_ARRAY) for better filtering, you'll need to parse it during ingestion.
                wc.Property(name="categories", data_type=wc.DataType.TEXT, description="Categories the product belongs to (comma-separated)"),
                wc.Property(name="salePrice", data_type=wc.DataType.NUMBER, description="Sale price of the product"),
                wc.Property(name="regularPrice", data_type=wc.DataType.NUMBER, description="Regular price of the product (if applicable)"),
                wc.Property(name="currency", data_type=wc.DataType.TEXT, description="Currency of the price"),
                wc.Property(name="imageURL", data_type=wc.DataType.TEXT, description="URL of the product image"),
                wc.Property(name="offerId", data_type=wc.DataType.TEXT, description="Unique identifier for the offer from the publisher"),
                wc.Property(name="publisher", data_type=wc.DataType.TEXT, description="Publisher of the offer (e.g., Action)"),
                wc.Property(name="validFrom", data_type=wc.DataType.DATE, description="Start date and time when the offer is valid from"),
                wc.Property(name="validTo", data_type=wc.DataType.DATE, description="End date and time when the offer is valid until"),
                wc.Property(name="scrapedAt", data_type=wc.DataType.DATE, description="Timestamp when the data was scraped"),
                wc.Property(
                    name="specs",
                    data_type=wc.DataType.OBJECT,
                    description="Specifications of the product",
                    nested_properties=[
                        wc.Property(name="size", data_type=wc.DataType.TEXT, description="Size or dimensions of the product (e.g., '100 ml', '300 ml', '250 g')"),
                        wc.Property(name="quantity", data_type=wc.DataType.INT, description="Number of items in a pack (e.g., '3 Stück', '50 Stück')"),
                        wc.Property(name="material", data_type=wc.DataType.TEXT, description="Material of the product (e.g., '100 % Baumwolle')"),
                        wc.Property(name="specialFeature", data_type=wc.DataType.TEXT, description="Any other notable feature from the description")
                    ]
                )
            ],
            # Configure the vectorizer module. Use 'text2vec-transformers' for local setup.
            # If using WCS with OpenAI, uncomment the OpenAI config.
            vectorizer_config=wc.Configure.Vectorizer.text2vec_transformers(), # For local with transformers module
            # vectorizer_config=wc.Configure.Vectorizer.text2vec_openai(), # For WCS with OpenAI
            # Add generative config if you plan to use Weaviate's generative capabilities (e.g., for RAG)
            # generative_config=wc.Configure.Generative.openai()
        )
        print(f"Collection '{collection_name}' created successfully.")

    except Exception as e:
        print(f"Error creating schema: {e}")

if __name__ == "__main__":
    client = get_weaviate_client()
    if client:
        create_product_offer_schema(client)
        client.close()