using System;
using System.IO;
using System.Text;
using System.IO.Compression;

namespace DOES.DataEngine.FileOperations
{
    /// <summary>
    /// This class handles exporting data from a database and writing it to a new data file on the local operating system. 
    /// </summary>
    public class ExportHandler
    {
        private string _folder;
        private Int32 _currentFileID;
        private Int32 _currentFile_PageIDCount;
        private Int32 _currentFile_FirstPageID;
        private string _rootFileName;
        private static Stream _gs;
        private static FileStream _fs;
        private static StreamWriter _sw;

        /// <summary>
        /// Instantiates a new Exporthandler to track the folder where the files will be created with associated properties. 
        /// </summary>
        public ExportHandler(string Folder, string fileNameRoot)
        {
            _folder = Folder;
            _rootFileName = fileNameRoot;
            _currentFileID = 1;
            _currentFile_FirstPageID = 0;
            _currentFile_PageIDCount = 0;
        }
        /// <summary>
        /// Returns the current file name for the export handler queue. 
        /// </summary>
        public string GetCurrentFileName { get { return _folder + @"\" + _rootFileName + _currentFileID.ToString("D4") + ".gz"; } }

        /// <summary>
        /// Creates and opens the file , returning a memory stream. 
        /// </summary>
        public void InitializeWriter()
        {
            _fs = new FileStream(GetCurrentFileName, FileMode.OpenOrCreate, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan);
            _gs = new GZipStream(_fs, CompressionMode.Compress);
            _sw = new StreamWriter(_gs, Encoding.UTF8);
        }

        /// <summary>
        /// Closes the stream. 
        /// </summary>
        public void CloseStreamWriter()
        {
            if (_sw != null)
            {
                _sw.Close();
            }
        }
        /// <summary>
        /// Checks the size of the underlying GzipStream object. 
        /// </summary>
        public Int64 CheckGZipStream()
        {
            Int64 position = new FileInfo(GetCurrentFileName).Length;
            return position;
        }

        /// <summary>
        /// Writes the webpage as a json string to a new file as a new line. 
        /// </summary>
        public bool WriteJsonToFile(string content)
        {
            bool sucess = false;
            _sw.WriteLine(content);
            return sucess;
        }

        /// <summary>
        /// Return the current file ID. 
        /// </summary>
        public Int32 CurrentFileID { get { return _currentFileID; } set { _currentFileID = value; } }
        /// <summary>
        /// Get the number of pages exported to a file. 
        /// </summary>
        public Int32 CurrentFile_PageIDCount { get { return _currentFile_PageIDCount; } set { _currentFile_PageIDCount = value; } }
        /// <summary>
        /// Return the first page ID to be written to the file. 
        /// </summary>
        public Int32 CurrentFile_FirstPageID { get { return _currentFile_FirstPageID; } set { _currentFile_FirstPageID = value; } }
    }
}
