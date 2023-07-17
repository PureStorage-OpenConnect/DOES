using System;
using System.IO;
using System.IO.Compression;
using System.Text;

namespace DOES.DataEngine.FileOperations
{
    /// <summary>
    /// This class handles reading from compressed data files. 
    /// </summary>
    public class ImportHandler
    {
        private string _fileName;
        private Int32 _importedWebPages;
        private bool _importedAllWebPages;
        private bool _found;
        private Stream _gs;
        private FileStream _fs;
        private StreamReader _sr;

        /// <summary>
        /// Instantiates the ImportHandler class for a specific file. 
        /// </summary>
        public ImportHandler(string filename, Int32 importedWebPages, bool importedAllWebPages, bool found)
        {
            _fileName = filename;
            _importedAllWebPages = importedAllWebPages;
            _importedWebPages = importedWebPages;
            _found = found;
        }

        /// <summary>
        /// Instantiates the reader to read the underlying data stream from each compressed file.  
        /// </summary>
        public void InitializeReader()
        {
            _fs = new FileStream(@_fileName, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
            _gs = new GZipStream(_fs, CompressionMode.Decompress);
            _sr = new StreamReader(_gs, Encoding.UTF8);
        }

        /// <summary>
        /// Closes all reader objects. 
        /// </summary>
        public void TerminateImportOperation()
        {
            _sr.Close();
            _gs.Close();
            _fs.Close();
        }

        /// <summary>
        /// Reads a single line from the data stream.  
        /// </summary>
        public string ReadFileLine()
        {
            string line = string.Empty;
            try
            {
                if (_sr == null)
                {
                    throw new Exception("There was an error reading the file " + _fileName);
                }
                if (!_sr.EndOfStream)
                {
                    line = _sr.ReadLine();
                }
                else
                {
                    _importedAllWebPages = true;
                }
            }
            catch(ObjectDisposedException)
            { }
            return line;
        }

        /// <summary>
        /// Checks if the end of the stream has been reached. 
        /// </summary>
        public bool EOF()
        {
            try
            {
                if (_sr == null)
                {
                    throw new Exception("There was an error reading the file " + _fileName);
                }
                else if (_sr.EndOfStream == true)
                {
                    _importedAllWebPages = true;
                    return _sr.EndOfStream;
                }
                else
                {
                    return _sr.EndOfStream;
                }
            }
            catch (ObjectDisposedException)
            {
                return true;
            }
            catch (Exception)
            {
                _importedAllWebPages = false;
                return true;
            }
        }
        /// <summary>
        /// Returns the filename 
        /// </summary>
        public string Filename { get { return _fileName; } set { _fileName = value; } }
        /// <summary>
        /// Returns the number of imported web pages. 
        /// </summary>
        public Int32 ImportedWebPages { get { return _importedWebPages; } set { _importedWebPages = value; } }
        /// <summary>
        /// Returns if all of the webpages for a specific file has been read. 
        /// </summary>
        public bool AllImportedWebPages { get { return _importedAllWebPages; } set { _importedAllWebPages = value; } }
        /// <summary>
        /// Checks if the data file has been found in the database historical tracking record. 
        /// </summary>
        public bool Found { get { return _found; } set { _found = value; } }
    }
}
