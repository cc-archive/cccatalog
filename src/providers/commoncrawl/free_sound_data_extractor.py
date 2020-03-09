import requests
from bs4 import BeautifulSoup
import logging
import json  
import argparse
import multiprocessing as mp

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.INFO)

# The URL for freesound archive
URL = "https://freesound.org"


def extract_titles_and_links (page=1): 
    """
    A function to extract all the sound files names and their links from the page number passed as the parameter. 

    The page number defaults to 1

    """
    logging.info("Scraping for page {}".format(page))
    
    url_for_page = URL + "/search/?page={}".format(page)

    logging.info("Downloading html page")
    html_page = requests.get(url_for_page)

    parsed_html_page = BeautifulSoup(html_page.content, 'html5lib')

    # Getting all the div tags with class small_player_small as it contains the sound link and title
    all_divisions = parsed_html_page.findAll('div', attrs={'class': 'sample_player_small'})
    
    # A dictionary to store the title as the key and the link as the value.
    titles_with_links = {}

    logging.info("Looping through all the div tags to find titles and links")

    for division in all_divisions: 
        try : 
            # Finding the a tags with class title  
            for inner_division in division.findAll('a', attrs = {'class': 'title'}): 
                if inner_division['title'] not in titles_with_links: 
                    titles_with_links[inner_division['title']] = inner_division['href']

        except: 
            pass
    
    logging.info("Succesfully extracted the titles and links from the page")
    
    return titles_with_links

def process_license (license_string): 
    """
    A function to extract the license form the whole string passed as function parameter 
    """
    return license_string.split('the')[1]

def extract_sound_meta (link_to_page): 
    """
    A function that takes the parameter to the link of a particular sound file and returns the 
    data about it from the page. 

    Extrarcts the following data : (the string in the paranthesis corresponds to the actual key of the dictionary returned)
        
        1. The file type (Type) 
        2. File duration (Duration)
        3. File Size (Filesize)
        4. Sample rate (Samplerate)
        5. Bit depth (Bitdepth)
        6. Channels (Channels)
        7. License (License)
        8. License Link (License Link)
        9. The author (Author)
        10. The link to the sound (Link)
        11. Tags for the sound (Tags)
        12. Description of the sound provided (Description)

    """

    # Link to the file generated after concatenating to the base URL
    link_for_file = URL + link_to_page

    # The returned dictionary
    meta = {}
    logging.info("Getting meta data for file : {}".format(link_to_page))
    html_page = requests.get(link_for_file)

    parsed_html_page = BeautifulSoup(html_page.content, 'html5lib')

    # Extracting the file type, duration, size, sample rate, bit depth, channels
    sound_information = parsed_html_page.find('dl', attrs = {'id': 'sound_information_box'})
    
    for key, value in zip(sound_information.findAll('dt'), sound_information.findAll('dd')) : 
        meta[key.text] = str(value.text)
    
    # Extracts license and license link 
    sound_license = parsed_html_page.find('div', attrs = {'id': 'sound_license'})
    meta['License'] = process_license(sound_license.a.text)
    meta['License Link'] = sound_license.a['href']

    # Extract author 
    sound_author = parsed_html_page.find('div', attrs = {'id': 'sound_author'}).a.text
    meta['Author'] = sound_author
    meta['Link'] = link_for_file

    tags = parsed_html_page.find('ul', attrs={'class': 'tags'})
    sound_tags = []
    for i in tags.findAll('li'): 
        sound_tags.append(i.a.text)
    
    meta['Tags'] = sound_tags

    # Extract description
    meta['Description'] = parsed_html_page.find('div', attrs={'id': 'sound_description'}).p.text

    return meta

def get_all_data_from_page(page) : 
    # All the details for the page
    page_output = {}
    page_output['Meta Data'] = {}

    # Current page meta
    currrent_meta = {}

    # Getting the Authors 
    authors = []

    logging.info("Getting the data from page number {}".format(page))

    try : 
        titles_and_links = extract_titles_and_links(page)
        currrent_meta["number of sounds"] = len(titles_and_links)
        for j in titles_and_links : 
            meta = extract_sound_meta(titles_and_links[j])
            authors.append(meta['Author'])
            page_output[j] = meta
            logging.info("Title : {} ; Link : {} \n".format(j, titles_and_links[j]))

            """for j in meta : 
                logging.info("{} : {}".format(j, meta[j]))
            logging.info('\n')"""
        
        # Putting the number of unique authors in the page
        currrent_meta['number of unique authours'] = len(set(authors))

        page_output['Meta Data'] = currrent_meta
        
        return (page_output)

    except : 
        pass
    
def get_total_pages (): 
    """
    Returns the total number of pages on freesound 
    """
    url_to_parse = URL + "/search/"
    html_page = requests.get(url_to_parse)
    parsed_html_page = BeautifulSoup(html_page.content, 'html5lib')
    total_pages = parsed_html_page.find('li', attrs={'class':'last-page'}).a.text
    return total_pages


def get_all_sounds (number_of_pages=10) : 
    """
    A function to loop through all the pages and get all data. The parameter is the total number of pages
    The default number is set to 10
    """
    
    # Final returned dictionary with the key as the title of sound and value as the meta dictionary returned from function 
    # extract_sound_data

    finalOutput = {}

    #Total number of files scraped
    number_of_files = 0

    #Initializing meta information
    finalOutput['Meta Data'] = {}


    logging.info("Number of processors: {}".format(mp.cpu_count()))
    pool = mp.Pool(mp.cpu_count())
    
    # Parallely getting data from all pages by assigning each page to a different thread
    page_outputs = pool.map(get_all_data_from_page, range(1,number_of_pages+1))
    
    for i in range (len(page_outputs)): 
        number_of_files += page_outputs[i]['Meta Data']['number of sounds']
        finalOutput["Page {}".format(i+1)] = page_outputs[i]
        

    # A dictionary to store the information such as number of files processed. 
    meta_for_final = {}
    meta_for_final['Number of pages scraped'] = len(finalOutput) - 1
    meta_for_final['Number of files'] = number_of_files

    finalOutput['Meta Data'] = meta_for_final

    return (finalOutput)

parser = argparse.ArgumentParser(
        description='Extract FreeSound data',
        add_help=True
    )

parser.add_argument(
    '--pages',
    help='Extract data for the number of pages provided. Provide argument as all for processing all pages')

args = parser.parse_args()

if args.pages:
    if (args.pages != 'all'): 
        number_of_pages = int(args.pages)
    else : 
        number_of_pages = int(get_total_pages())
else : 
    number_of_pages = 3

logging.info("The total number of pages is : {}".format(get_total_pages()))

logging.info("Extracting the data for {} pages".format(number_of_pages))

all_data = get_all_sounds(number_of_pages=number_of_pages)

# Serializing json  
json_object = json.dumps(all_data, indent = 4) 
    
# Writing to output.json 
with open("output.json", "w") as outfile: 
    outfile.write(json_object)
