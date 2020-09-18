#from textblob import TextBlob
#from sentimentanalyzer.sentimentanalyzer import Sentiment
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
text = "This is a great product"

text = """
UBS today reaffirms its positive investment rating on Spirent Communications PLC (LON:SPT) and raised its price target to 265p (from 220p).

"""

# vader

analyzer = SentimentIntensityAnalyzer()
r = analyzer.polarity_scores(text)
print(r)
