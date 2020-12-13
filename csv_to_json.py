
import csv 
import json 


# Function to convert a CSV to JSON 
# Takes the file paths as arguments 
def make_json(csvFilePath, jsonFilePath): 
	
	data_list = []
	# create a dictionary 
	data = {} 
	
	# Open a csv reader called DictReader 
	with open(csvFilePath) as csvf: 
		csvReader = csv.DictReader(csvf) 
		
		# Convert each row into a dictionary 
		# and add it to data 
		for row in csvReader: 
			data_list.append(row)

	# Open a json writer, and use the json.dumps() 
	# function to dump data 
	with open(jsonFilePath, 'w') as jsonf: 
		jsonf.write(json.dumps(data_list, indent=4))

if __name__=='__main__':
	make_json("pricing.csv","pricing.json")
	print("done writing")
