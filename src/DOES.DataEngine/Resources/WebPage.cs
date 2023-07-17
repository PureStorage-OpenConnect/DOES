using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;

namespace DOES.DataEngine.Resources
{
    /// <summary>
    /// A class to contain web page data in multiple forms. 
    /// </summary>
    [JsonObject(MemberSerialization.OptIn)]
    public class WebPage
    {
        private int _pageID;
        private DateTime _loadedOn;
        private DateTime _modifiedOn;
        private int _updates;
        private int _HREFs;
        private int _hashURL;
        private byte[] _hashHTML;
        private string _URL;
        private byte[] _HTMLBinary;
        private string _HTML;
        private SortedList<string, string> _headers;
        private int _amplifier;

        private byte[][] _dataAsBytes;
        private byte[][] _dataAsBS64s;
        private byte[][] _statsAsBytes;
        private byte[][] _statsAsBS64s;

        private string _URL_B64S;
        private string _HTML_B64S;

        private ComputedLengths _computedLengths;

        /// <summary>
        /// Instantiates the base web page class without any data added. 
        /// </summary>
        public WebPage()
        {

        }

        /// <summary>
        /// Instantiates the base web page class with a page ID and the lengths. 
        /// </summary>
        public WebPage(int PageID, ComputedLengths compLength)
        {
            _pageID = PageID;
            _computedLengths = compLength;
        }

        /// <summary>
        /// Instantiates the base web page class with basic web page data.  
        /// </summary>
        public WebPage(int PageID, string URL, string htmlText, SortedList<string, string> headers)
        {
            _pageID = PageID;
            _URL = URL;
            _HTML = htmlText;
            _headers = headers;
        }

        /// <summary>
        ///  Instantiates the base web page class with compete web page data. 
        /// </summary>
        public WebPage(int pageID, int amplifier, string URL, string HTML, SortedList<string, string> headers, 
            int HREFS, int HashURL, byte[] HTMLBinary, byte[] HashHTML, byte[][] DataAsBytes, byte[][] DataAsBS64s,
           byte[][] StatsAsBytes, byte[][] StatsAsBS64s, ComputedLengths compLength)
        {
            _pageID = PageID;
            _amplifier = amplifier;
            _URL = URL;
            _HTML = HTML;
            _headers = headers;
            _HREFs = HREFS;
            _hashURL = HashURL;
            _HTMLBinary = HTMLBinary;
            _hashHTML = HashHTML;
            _dataAsBytes = DataAsBytes;
            _dataAsBS64s = DataAsBS64s;
            _statsAsBytes = StatsAsBytes;
            _statsAsBS64s = StatsAsBS64s;
            _computedLengths = compLength;
        }


        /// <summary>
        /// Returns the page ID. 
        /// </summary>
        [JsonProperty]
        public int PageID { get { return _pageID; } set { _pageID = value; } }
        /// <summary>
        /// Returns when the web page was loaded on. 
        /// </summary>
        public DateTime LoadedOn { get { return _loadedOn; } set { _loadedOn = value; } }
        /// <summary>
        /// Returns when the web page was last modified. 
        /// </summary>
        public DateTime ModifiedOn { get { return _modifiedOn; } set { _modifiedOn = value; } }
        /// <summary>
        /// Returns the number of updates done to the web page. 
        /// </summary>
        public int Updates { get { return _updates; } set { _updates = value; } }
        /// <summary>
        /// Return the length of all of the headers for the web page. 
        /// </summary>
        public UInt64 HeadersLength { get { return _computedLengths.HeadersLengths; } 
            set { _computedLengths.HeadersLengths = value; } }
        /// <summary>
        /// Return the length of the statistics for the web page. 
        /// </summary>
        public UInt64 StatsLength { get { return _computedLengths.StatsLengths; } 
            set { _computedLengths.StatsLengths = value; } }
        /// <summary>
        /// Return the total length of the web page.
        /// </summary>
        public UInt64 TotalLength { get { return _computedLengths.TotalLenghts; } 
            set { _computedLengths.TotalLenghts = value; } }
        /// <summary>
        /// Return the number of header references for the web page. 
        /// </summary>
        public int HREFS { get { return _HREFs; } set { _HREFs = value; } }
        /// <summary>
        /// Returns the hash of the URL.  
        /// </summary>
        public int HashURL { get { return _hashURL; } set { _hashURL = value; } }
        /// <summary>
        /// Return the Hash of the HTML of the web page. 
        /// </summary>
        public byte[] HashHTML { get { return _hashHTML; } set { _hashHTML = value; } }
        /// <summary>
        /// Return the URL of the web page. 
        /// </summary>
        [JsonProperty]
        public string URL { get { return _URL; } set { _URL = value; } }
        /// <summary>
        /// Returns the HTML in an  encoded byte array. 
        /// </summary>
        public byte[] HTMLBinary { get { return _HTMLBinary; } set { _HTMLBinary = value; } }
        /// <summary>
        /// Return the HTML of the web page. 
        /// </summary>
        [JsonProperty]
        public string HTML { get { return _HTML; } set { _HTML = value; } }
        /// <summary>
        /// Return a sorted list of the headers of a web page. 
        /// </summary>
        [JsonProperty]
        public SortedList<string, string> Headers { get { return _headers; } set { _headers = value; } }
        /// <summary>
        /// Return a 2 dimensional byte array of the HTML object. 
        /// </summary>
        public byte[][] DataAsBytes { get { return _dataAsBytes; } set { _dataAsBytes = value; } }
        /// <summary>
        /// Return a 2 dimensional byte array of the base 64 string HTML object. 
        /// </summary>
        public byte[][] DataAsBS64s { get { return _dataAsBS64s; } set { _dataAsBS64s = value; } }
        /// <summary>
        /// Return a 2 dimensional byte array of the statistics HTML object. 
        /// </summary>
        public byte[][] StatsAsBytes { get { return _statsAsBytes; } set { _statsAsBytes = value; } }
        /// <summary>
        /// Returns a 2 dimensional array of the statistics HTML base 64 string object. 
        /// </summary>
        public byte[][] StatsAsBS64s { get { return _statsAsBS64s; } set { _statsAsBS64s = value; } }
        /// <summary>
        /// Return the URL Base64 String of the web page. 
        /// </summary>
        [JsonProperty]
        public string URLB64S { get { return _URL_B64S; } set { _URL_B64S = value; } }
        /// <summary>
        /// Return the HTML Base64 string of the web page. 
        /// </summary>
        [JsonProperty]
        public string HTMLB64S { get { return _HTML_B64S; } set { _HTML_B64S = value; } }
    }

    /// <summary>
    /// This class contains the computed lengths for the web page data. 
    /// </summary>
    public class ComputedLengths
    {
        private UInt64 _statsLength;
        private UInt64 _headersLength;
        private UInt64 _totalLength;

        /// <summary>
        /// Instantiates the class by assigning the length values. 
        /// </summary>
        public ComputedLengths(UInt64 StatsLengths, UInt64 HeadersLength, UInt64 TotalLength)
        {
            _statsLength = StatsLengths;
            _headersLength = HeadersLength;
            _totalLength = TotalLength;
        }
        /// <summary>
        /// Instantiates the class with only the total length. 
        /// </summary>
        public ComputedLengths(UInt64 TotalLength)
        {
            _totalLength = TotalLength;
        }
        /// <summary>
        /// Returns the statistics length. 
        /// </summary>
        public UInt64 StatsLengths { get { return _statsLength; } 
            set { _statsLength = value; } }
        /// <summary>
        /// Returns the header length. 
        /// </summary>
        public UInt64 HeadersLengths { get { return _headersLength; } 
            set { _headersLength = value; } }
        /// <summary>
        /// Returns the total length.
        /// </summary>
        public UInt64 TotalLenghts { get { return _totalLength; } 
            set { _totalLength = value; } }
    }
}
