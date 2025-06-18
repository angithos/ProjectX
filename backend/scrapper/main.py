import requests
import pandas as pd
from datetime import datetime
import json
import os


# Get all the supermarkets
def get_data():
    # Target URL and parameters
    url = "https://www.kaufda.de/webapp/api/slots/shelf"
    params = {
        "version": "",
        "lat": "52.4251",
        "lng": "13.5425",
        "size": "24",
        "page": "0"
    }

    # Required headers
    headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.5",
        "bonial_account_id": "a0671c72-f587-4a6b-85e8-416b2db493ea",
        "content-type": "application/json",
        "delivery_channel": "dest.kaufda",
        "priority": "u=1, i",
        "referer": "https://www.kaufda.de/webapp/?lat=52.4251&lng=13.5425&zip=12489",
        "sec-ch-ua": '"Brave";v="137", "Chromium";v="137", "Not/A)Brand";v="24"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
        "user_platform_category": "desktop.web.browser",
        "user_platform_os": "linux"
    }

    # Cookies with sessionToken and location
    cookies = {
        "location": '{"lat":52.4251,"lng":13.5425,"city":"","cityUrlRep":"","zip":"12489"}',
        "sessionToken": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJzZXNzaW9uIjoiMDlmYjllNzEtMGZmOS00MTY3LWI3OWMtMGJjZDczYzAxZDllIiwiaXNzIjoidXNlci1tYW5hZ2VtZW50Iiwib3B0ZWRfb3V0Ijp0cnVlLCJleHAiOjE3NDk2NjUwNzksImlhdCI6MTc0OTY2MzI3OSwidXNlciI6IjA5ZmI5ZTcxLTBmZjktNDE2Ny1iNzljLTBiY2Q3M2MwMWQ5ZSJ9.b08_OUJZGiuP4rmOK9nZWprb3beQwyjOX1lwFG6GkjddJvButYizaby-6ZlsWFhDftCRnFjf_gS8s84ABokUos2ZmkPjNCyA0potawnhOv4ObrjnwRSpToqee5dOFq4D_nrXzZahKBtqu6Txr-4Z9Nw9WVanCaiZiml_b4nb5SL8wslsgTjKbaQ7PQqJNOV95G3r_8jGVNKfW98ruTbBW3enqzxRrqcghnVkJ_oJ0zXZMIIbNP2k18gR9fAQLg6QDvhWnHCwKPyhZfEB1WfLOtbF0XA-0oIpEHxBygh0lo31kiflFAP8swbrm_45ZCk5kmb_bK1Nd0EWUzb6FZ1-Ow"
    }

    # Send request
    print("📡 Fetching brochures from page 0...")
    response = requests.get(url, headers=headers, params=params, cookies=cookies)

    # Process and save response
    try:
        response.raise_for_status()
        data = response.json()
        with open("brochures.json", "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print("✅ Saved data to brochures.json")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")

# Store the products
def find_products():
    # Define the name of the JSON file
    file_name = "brochures.json"

    # Initialize an empty list to store the extracted data
    extracted_data = []

    try:
        # Check if the file exists before attempting to open it
        if not os.path.exists(file_name):
            print(f"Error: The file '{file_name}' was not found in the current directory.")
            print("Please make sure '{file_name}' is in the same directory as this script.")
        else:
            # Open the JSON file for reading
            # Using 'with' ensures the file is automatically closed after its block
            with open(file_name, 'r', encoding='utf-8') as f:
                # Load the JSON data from the file into a Python dictionary
                data = json.load(f)

            # Access the 'contents' list. Use .get() with an empty list default
            # to prevent errors if '_embedded' or 'contents' keys are missing.
            contents = data.get("_embedded", {}).get("contents", [])

            print("Starting data extraction and storage...")
            print("------------------------------------")
            # Loop through each item in the 'contents' list
            for i, item in enumerate(contents):
                # Print which item is currently being processed for debugging/tracking
                print(f"Processing Item {i+1}:")

                # --- Robustness Check 1: Ensure 'item' is a dictionary ---
                if not isinstance(item, dict):
                    print(f"  Warning: Item {i+1} is not a dictionary (it's type: {type(item)}). Skipping this item.")
                    print("-" * 36)
                    continue # Skip to the next item if it's not a dictionary

                # Safely get the 'content' dictionary from the current item
                # Default to an empty dictionary if 'content' key is not found
                content_info = item.get("content", {})

                # --- Robustness Check 2: Ensure 'content_info' is a dictionary ---
                if not isinstance(content_info, dict):
                    print(f"  Warning: 'content' for Item {i+1} is not a dictionary (it's type: {type(content_info)}). Skipping this item.")
                    print("-" * 36)
                    continue # Skip if 'content_info' is not a dictionary

                # Safely get the 'contentId' from the 'content_info' dictionary
                # Default to None if 'contentId' key is not found
                content_id = content_info.get("contentId")

                # Safely get the 'publisher' dictionary from the 'content_info' dictionary
                # Default to None if 'publisher' key is not found
                publisher_info = content_info.get("publisher")

                # Create a dictionary for the current item's extracted data
                item_data = {
                    "contentId": content_id,
                    "publisher": publisher_info
                }

                # Add the extracted item data to our list
                extracted_data.append(item_data)

                # Print extracted information for confirmation
                if content_id:
                    print(f"  Content ID: {content_id}")
                else:
                    print("  Content ID: Not found")

                if publisher_info:
                    print(f"  Publisher: {publisher_info}")
                else:
                    print("  Publisher: Not found")
                print("-" * 36)

            # After the loop, print the entire collected data
            print("\n--- All Extracted Data ---")
            # Use json.dumps for pretty printing the list of dictionaries
            print(json.dumps(extracted_data, indent=2))
            print(f"\nTotal items extracted: {len(extracted_data)}")

    except json.JSONDecodeError as e:
        # Handle errors that occur if the file content is not valid JSON
        print(f"\nError decoding JSON from '{file_name}': {e}")
        print("This error usually means the JSON file is incomplete, malformed, or has syntax errors.")
        print("Please carefully check your '{file_name}' file for missing brackets, commas, or other JSON syntax issues.")
    except KeyError as e:
        # Handle cases where top-level expected keys are missing in the JSON structure
        print(f"\nMissing expected top-level key in JSON structure: {e}")
        print("The JSON structure in '{file_name}' might be missing '_embedded' or 'contents'.")
    except Exception as e:
        # Catch any other unexpected errors
        print(f"\nAn unexpected error occurred: {e}")
        print("This could be due to a more complex")
    
    return extracted_data

# Gathers all the data and stores it into a csv with all the data
def scrape_kaufda_products(id, store):
    # API endpoint URL
    api_url = f"https://www.kaufda.de/webapp/api/brochures/{id}"
    
    # Parameters from the XHR request
    params = {
        "brochureId": id,
        "lat": "52.4251",
        "lng": "13.5425",
        "partner": "kaufda_web",
        "brochureKey": "",
        "userPlatformCategory": "desktop.web.browser",
        "userPlatformOs": "linux",
        "adPlacementSource": "ad_placement__shelf_fixed_position_1"
    }
    
    # Headers to mimic a browser request
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.kaufda.de/",
    }
    
    try:
        # Send GET request to the API
        response = requests.get(api_url, params=params, headers=headers)
        response.raise_for_status()
        
        # Parse JSON response
        data = response.json()
        
        # Extract offers
        offers = data.get('offers', {}).get('_embedded', {}).get('contents', [])
        
        products = []
        for offer in offers:
            # Get all products in this offer
            offer_products = offer.get('products', [])
            
            for product in offer_products:
                # Get the best deal (lowest price)
                deals = offer.get('deals', [])
                sale_price = None
                regular_price = None
                
                for deal in deals:
                    if deal.get('type') == 'SALES_PRICE':
                        sale_price = deal.get('min')
                    elif deal.get('type') == 'REGULAR_PRICE':
                        regular_price = deal.get('min')
                
                # Get first image URL if available
                image_url = None
                if product.get('images') and len(product['images']) > 0:
                    image_url = product['images'][0].get('url')
                
                # Get categories as string
                categories = ", ".join(product.get('categories', []))
                
                # Create product info
                product_info = {
                    "name": product.get('name'),
                    "brand": product.get('brand', {}).get('name'),
                    "description": product.get('description', [{}])[0].get('paragraph', ''),
                    "categories": categories,
                    "sale_price": sale_price,
                    "regular_price": regular_price,
                    "currency": "EUR",
                    "image_url": image_url,
                    "offer_id": offer.get('id'),
                    "publisher": offer.get('publisherName'),
                    "valid_from": offer.get('publicationProfiles', [{}])[0].get('validity', {}).get('startDate'),
                    "valid_to": offer.get('publicationProfiles', [{}])[0].get('validity', {}).get('endDate'),
                    "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                products.append(product_info)
        
        # Create DataFrame
        df = pd.DataFrame(products)
        
        if df.empty:
            print("Warning: No products found in the response.")
            return False
        
        # Save to CSV
        # timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        # csv_filename = f"{store}_products_{timestamp}.csv"
        # df.to_csv(csv_filename, index=False, encoding="utf-8")
        
        # print(f"Successfully scraped {len(products)} products. Saved to {csv_filename}")
        # return True
        # Create the scrapped folder if it doesn't exist
        os.makedirs("scrapped", exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_filename = f"scrapped/{store}_products_{timestamp}.csv"  # Add folder to path
        df.to_csv(csv_filename, index=False, encoding="utf-8")

        print(f"Successfully scraped {len(products)} products. Saved to {csv_filename}")
        return True


    
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {e}")
    except ValueError as e:
        print(f"Error parsing JSON: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    return False

# Run the scraper
if __name__ == "__main__":

    if os.path.exists("brochures.json"):
        print(f"The file '{"brochures.json"}' exists. Skipping call for data.")
    else:
        get_data()
    
    data = find_products()
    for x in data:
        scrape_kaufda_products(x['contentId'],x["publisher"]["name"] )