using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Files
{
   internal class FileModificationRef
   {
      private FileInfo _fileInfo;
      private long _fileLength;
      private long _deleteLength;

      public FileModificationRef( FileInfo file, long fileLength, long deleteLength )
      {
         _fileInfo = file;
         _fileLength = fileLength;
         _deleteLength = deleteLength;
      }

      public long DeleteLength
      {
         get
         {
            return _deleteLength;
         }
      }

      public void PerformModification()
      {
         if( _deleteLength == _fileLength )
         {
            _fileInfo.Delete();
         }
         else
         {
            QSFile.DeleteFilePart( 
               _fileInfo.Open( FileMode.Open, FileAccess.ReadWrite ), 
               0,
               _deleteLength );
         }
      }
   }
}
