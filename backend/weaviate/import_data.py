# ingest_data.py
import pandas as pd
from datetime import datetime
import re
import weaviate
from client import get_weaviate_client # Import the client function

# Function to extract specs from description (replicated from previous response for clarity)
def extract_specs_from_description(description):
    specs = {
        "size": None,
        "quantity": None,
        "material": None,
        "specialFeature": None
    }

    if not description:
        return specs

    # Make a mutable copy for parsing
    working_description = description

    # Example: "100 ml", "300 ml", "3,9 ml", "250 g", "190 x 300 x 125 cm"
    size_match = re.search(r'(\d+(\.\d+)?\s*(ml|g|cm|x\s*\d+\s*cm)\b)', working_description, re.IGNORECASE)
    if size_match:
        specs["size"] = size_match.group(1).strip()
        working_description = working_description.replace(size_match.group(0), '', 1).strip()

    # Example: "3 Stück", "50 Stück", "2 St"
    quantity_match = re.search(r'(\d+)\s*(Stück|St)\b', working_description, re.IGNORECASE)
    if quantity_match:
        try:
            specs["quantity"] = int(quantity_match.group(1))
            working_description = working_description.replace(quantity_match.group(0), '', 1).strip()
        except ValueError:
            pass

    # Example: "100 % Baumwolle", "Kunststoff", "Velours-Stoff", "ABS-Kunststoff"
    material_match = re.search(r'(100\s*%\s*Baumwolle|Kunststoff|Velours-Stoff|ABS-Kunststoff)', working_description, re.IGNORECASE)
    if material_match:
        specs["material"] = material_match.group(1).strip()
        working_description = working_description.replace(material_match.group(0), '', 1).strip()

    # The rest of the description can be a special feature
    if working_description:
        specs["specialFeature"] = working_description.strip()

    return specs


def ingest_products_to_weaviate(products_data):
    """
    Ingests a list of product dictionaries into the 'ProductOffer' collection in Weaviate.
    """
    client = get_weaviate_client()
    if not client:
        return False

    product_offers_collection = client.collections.get("ProductOffer")

    try:
        with product_offers_collection.batch.dynamic() as batch: # Use dynamic batching
            for product_info in products_data:
                # Prepare data for Weaviate ingestion, matching schema
                data_object = {
                    "name": product_info.get('name'),
                    "brand": product_info.get('brand'),
                    "description": product_info.get('description'),
                    "categories": product_info.get('categories'), # Keep as TEXT for now
                    "salePrice": product_info.get('sale_price'),
                    "regularPrice": product_info.get('regular_price'),
                    "currency": product_info.get('currency'),
                    "imageURL": product_info.get('image_url'),
                    "offerId": product_info.get('offer_id'),
                    "publisher": product_info.get('publisher'),
                    "validFrom": product_info.get('valid_from'), # Already in ISO format from scraper
                    "validTo": product_info.get('valid_to'),     # Already in ISO format from scraper
                    "scrapedAt": product_info.get('scraped_at'), # Already in ISO format from scraper
                    "specs": extract_specs_from_description(product_info.get('description', ''))
                }

                # Handle empty strings for numerical fields by setting them to None
                # Weaviate expects numbers for NUMBER data type, not empty strings
                if data_object["salePrice"] == "":
                    data_object["salePrice"] = None
                if data_object["regularPrice"] == "":
                    data_object["regularPrice"] = None

                # Weaviate expects None for empty image_url, not empty string
                if data_object["imageURL"] == "":
                    data_object["imageURL"] = None
                
                # Weaviate expects None for empty brand
                if data_object["brand"] == "":
                    data_object["brand"] = None

                batch.add_object(
                    properties=data_object,
                    uuid=data_object["offerId"] # Use offerId as the UUID for idempotency
                )
        
        # Check batch results
        if batch.num_objects_class("ProductOffer") > 0:
            print(f"Successfully ingested {batch.num_objects_class('ProductOffer')} objects into Weaviate.")
            for error in batch.failed_objects:
                print(f"Failed to import object: {error.message} (Object: {error.object_})")
            return True
        else:
            print("No objects were added to the batch.")
            return False

    except weaviate.exceptions.WeaviateConnectionError as e:
        print(f"Weaviate connection error during ingestion: {e}")
        return False
    except Exception as e:
        print(f"An error occurred during ingestion: {e}")
        return False
    finally:
        if client:
            client.close()

if __name__ == "__main__":
    # This block is for testing ingestion directly with dummy data or a CSV
    print("This file is meant to be called by scraper.py with actual data.")
    print("You can add a test case here to read a local CSV and ingest.")

    # Example: Ingest data from a sample CSV (if you have one)
    # try:
    #     df = pd.read_csv("path/to/your/scrapped/some_products.csv")
    #     products_list = df.to_dict(orient='records')
    #     ingest_products_to_weaviate(products_list)
    # except FileNotFoundError:
    #     print("Test CSV not found.")
    # except Exception as e:
    #     print(f"Error reading test CSV: {e}")