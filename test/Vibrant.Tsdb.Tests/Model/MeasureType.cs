using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Vibrant.Tsdb.Tests.Model
{
   public class MeasureType : IMeasureType
   {
      private IFieldInfo[] _fields = new IFieldInfo[] { new FieldInfo( "Value", typeof( double ) ) };
      private string[] _tags = new [] { "Placement", "Owner" };
      private string _name;

      public MeasureType( string name )
      {
         _name = name;
      }

      public IEnumerable<IFieldInfo> GetFields()
      {
         return _fields;
      }

      public string GetName()
      {
         return _name;
      }

      public IEnumerable<string> GetTags()
      {
         return _tags;
      }
   }
}
