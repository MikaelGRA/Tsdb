using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb
{
   public class NullTsdbLogger : ITsdbLogger
   {
      public static readonly NullTsdbLogger Default = new NullTsdbLogger(); 

      public void Debug( string message )
      {
      }

      public void Debug( Exception e, string message )
      {
      }

      public void Error( string message )
      {
      }

      public void Error( Exception e, string message )
      {
      }

      public void Fatal( string message )
      {
      }

      public void Fatal( Exception e, string message )
      {
      }

      public void Info( string message )
      {
      }

      public void Info( Exception e, string message )
      {
      }

      public void Warn( string message )
      {
      }

      public void Warn( Exception e, string message )
      {
      }
   }
}
