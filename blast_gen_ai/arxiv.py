import requests
import xml.etree.ElementTree as ET
import pyspark.sql.functions as F

def search_arxiv_papers(spark, keyword="", max_results=None):
  if max_results is None:
    max_results = 50
  
  if keyword == "":
    return
  
  # Search ArXiv
  base_url = "http://export.arxiv.org/api/query"
  query = f"search_query=all:{keyword.replace(' ','+')}&searchtype=all&start=0&max_results={max_results}&source=header&orderby=relevance"
  url = f"{base_url}?{query}"
  response = requests.get(url)

  print(query)

  # Parse the XML data
  root = ET.fromstring(response.content)

  data = []
  for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
      title = entry.find('{http://www.w3.org/2005/Atom}title').text
      url = entry.find('{http://www.w3.org/2005/Atom}id').text
      data.append({"title": title, "url": url})

  # Create a dataframe
  df = (
    spark.createDataFrame(data)
    .withColumn('unique_id', F.regexp_replace('url', 'http://arxiv.org/abs/',''))
    .dropDuplicates(['unique_id'])
  )

  return df