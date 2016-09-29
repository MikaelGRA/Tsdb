using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Ats
{
   public class YearlyTable : ITable, IEquatable<YearlyTable>
   {
      private int _year;
      private string _yearString;

      public YearlyTable( int year )
      {
         _year = year;
         _yearString = year.ToString( CultureInfo.InvariantCulture );
      }

      public DateTime From
      {
         get
         {
            return new DateTime( _year, 0, 0, 0, 0, 0, DateTimeKind.Utc );
         }
      }

      public DateTime To
      {
         get
         {
            return new DateTime( _year + 1, 0, 0, 0, 0, 0, DateTimeKind.Utc );
         }
      }

      public string Suffix
      {
         get
         {
            return _yearString;
         }
      }

      public YearlyTable GetPrevious()
      {
         return new YearlyTable( _year - 1 );
      }

      // override object.Equals
      public override bool Equals( object obj )
      {
         var o = obj as YearlyTable;
         return o != null && Equals( o );
      }

      // override object.GetHashCode
      public override int GetHashCode()
      {
         return _year;
      }

      public bool Equals( ITable other )
      {
         var o = other as YearlyTable;
         return o != null && Equals( o );
      }

      public bool Equals( YearlyTable other )
      {
         return _year == other._year;
      }
   }
}
