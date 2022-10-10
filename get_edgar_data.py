import json
import urllib.request
import xml.etree.ElementTree as ET
import re
import os
import time
from urllib.request import Request, urlopen
from pandas.io.json import json_normalize
import numpy as np

TOKEN = "bbad310f3e1a062c26ffa525f3ea21f202dbe0d005ddcb6c171ec4c63ed555e4"  # replace YOUR_API_KEY with the API key you got from sec-api.io after sign up
# API endpoint
API = "https://api.sec-api.io?token=" + TOKEN


class GetEdgarData:
    def __init__(self) -> None:
        pass

    @classmethod
    def _get_edgar_data(cls, date):
        """
        date: str, e.g. 2020-12-30
        """
        filter = f'formType:"4" AND formType:(NOT "N-4") AND formType:(NOT "4/A") AND filedAt: [1997-01-01 TO {date}]'
        # Start with the first filing. Increase it when paginating.
        # Set to 10000 if you want to fetch the next batch of filings. Set to 20000 for the next and so on.
        start = 0
        # Return 10,000 filings per API call
        size = 10000
        # Sort in descending order by filedAt
        sort = [{"filedAt": {"order": "desc"}}]

        payload = {
            "query": {"query_string": {"query": filter}},
            "from": start,
            "size": size,
            "sort": sort,
        }

        # Format your payload to JSON bytes
        jsondata = json.dumps(payload)
        jsondataasbytes = jsondata.encode("utf-8")  # needs to be bytes

        # Instantiate the request
        req = urllib.request.Request(API)

        # Set the correct HTTP header: Content-Type = application/json
        req.add_header("Content-Type", "application/json; charset=utf-8")
        # Set the correct length of your request
        req.add_header("Content-Length", len(jsondataasbytes))

        # Send the request to the API
        response = urllib.request.urlopen(req, jsondataasbytes)

        # Read the response
        res_body = response.read()
        # Transform the response into JSON
        filingsJson = json.loads(res_body.decode("utf-8"))
        filings = cls.compress_filings(filingsJson["filings"])
        filings = cls.add_non_derivative_transaction_amounts(filings)
        filings = json_normalize(filings)
        # Store dataframe
        if not os.path.exists("./edgar_data/"):
            os.mkdir("./edgar_data/")
        filings.to_csv(f"./edgar_data/{date}.csv")

        print(
            "start period: ",
            filings["filedAt"].max(),
            ", end period: ",
            filings["filedAt"].min(),
        )
        time.sleep(5)

    @staticmethod
    def compress_filings(filings):
        store = {}
        compressed_filings = []
        for filing in filings:
            if filing["ticker"] == "":  # only download public company
                continue
            filedAt = filing["filedAt"]
            if filedAt in store:
                compressed_filings.append(filing)
                store[filedAt] += 1
            elif filedAt not in store:
                compressed_filings.append(filing)
                store[filedAt] = 1
        return compressed_filings

    @staticmethod
    def download_xml(url, tries=1, writeName=None):
        try:
            req = Request(url=url, headers={"User-Agent": "Ryan Corner/3.0"})
            response = urlopen(req)
        except Exception as e:
            print(e)
            print("Something went wrong for url: ", url)
            # print("Something went wrong. Wait for 5 seconds and try again.", tries)
            # if tries < 5:
            #     time.sleep(5 * tries)
            #     download_xml(url, tries + 1)
            return None
        else:
            # decode the response into a string
            data = response.read().decode("utf-8")
            # set up the regular expression extractoer in order to get the relevant part of the filing
            matcher = re.compile(
                "<\?xml.*ownershipDocument>", flags=re.MULTILINE | re.DOTALL
            )
            matches = matcher.search(data)
            # the first matching group is the extracted XML of interest
            try:
                xml = matches.group(0)
                # instantiate the XML object
                root = ET.fromstring(xml)
                if not os.path.exists("./edgar_xml/"):
                    os.mkdir("./edgar_xml/")

                if not writeName is None:
                    with open(f"./edgar_xml/{writeName}.xml", "w") as f:  #
                        # store downloaded xml files
                        f.write(xml)
                # print(url)
                return root
            except Exception as e:
                print(f"download_xml {url} Exception: ", e)
                return None

    @classmethod
    def add_non_derivative_transaction_amounts(cls, filings):
        for filing in filings:
            url = filing["linkToTxt"]
            writeName = filing["filedAt"] + "_" + filing["companyName"]
            writeName = writeName.replace(":", "")
            writeName = writeName.replace("/", "")
            xml = cls.download_xml(url, writeName=writeName)
            filing["nonDerivativeTransactions"] = ParseXML.calc_transaction_amount(xml)
        return filings


class ParseXML:
    @staticmethod
    def calc_transactionAmounts(xml):
        """Calculate the total transaction amount in $ of a giving form 4 in XML"""
        total = 0

        if xml is None:
            return total

        nonDerivativeTransactions = xml.findall(
            "./nonDerivativeTable/nonDerivativeTransaction"
        )

        for t in nonDerivativeTransactions:
            # D for disposed or A for acquired
            action = t.find(
                "./transactionAmounts/transactionAcquiredDisposedCode/value"
            ).text
            # number of shares disposed/acquired
            shares = t.find("./transactionAmounts/transactionShares/value").text
            # price
            priceRaw = t.find("./transactionAmounts/transactionPricePerShare/value")
            price = 0 if priceRaw is None else priceRaw.text
            # set prefix to -1 if derivatives were disposed. set prefix to 1 if derivates were acquired.
            prefix = -1 if action == "D" else 1
            # calculate transaction amount in $
            amount = prefix * float(shares) * float(price)
            total += amount

        return round(total, 2)

    @staticmethod
    def calc_transactionPricePerShare(xml):
        """Calculate the avg transaction price per share in $ of a giving form 4 in XML"""
        if xml is None:
            return np.nan

        nonDerivativeTransactions = xml.findall(
            "./nonDerivativeTable/nonDerivativeTransaction"
        )

        prices = []
        shares = []
        for t in nonDerivativeTransactions:
            action = t.find(
                "./transactionAmounts/transactionAcquiredDisposedCode/value"
            ).text
            shareRaw = t.find("./transactionAmounts/transactionShares/value").text
            priceRaw = t.find("./transactionAmounts/transactionPricePerShare/value")
            if not priceRaw is None:
                prices.append(float(priceRaw.text) * float(shareRaw))
                shares.append(float(shareRaw))

        if len(prices) > 0:
            return np.sum(prices) / np.sum(shares)
        else:
            return np.nan

    @staticmethod
    def calc_transactionShares(xml):
        """Calculate the avg transaction price per share in $ of a giving form 4 in XML"""
        if xml is None:
            return np.nan

        nonDerivativeTransactions = xml.findall(
            "./nonDerivativeTable/nonDerivativeTransaction"
        )

        shares = []
        for t in nonDerivativeTransactions:
            action = t.find(
                "./transactionAmounts/transactionAcquiredDisposedCode/value"
            ).text
            shareRaw = t.find("./transactionAmounts/transactionShares/value")
            prefix = -1 if action == "D" else 1
            if not shareRaw is None:
                shares.append(prefix * float(shareRaw.text))

        if len(shares) > 0:
            return np.sum(shares)
        else:
            return np.nan

    @staticmethod
    def calc_absTransactionShares(xml):
        """Calculate the avg transaction price per share in $ of a giving form 4 in XML"""
        if xml is None:
            return np.nan

        nonDerivativeTransactions = xml.findall(
            "./nonDerivativeTable/nonDerivativeTransaction"
        )

        shares = []
        for t in nonDerivativeTransactions:
            shareRaw = t.find("./transactionAmounts/transactionShares/value")
            if not shareRaw is None:
                shares.append(float(shareRaw.text))

        if len(shares) > 0:
            return np.sum(shares)
        else:
            return np.nan
