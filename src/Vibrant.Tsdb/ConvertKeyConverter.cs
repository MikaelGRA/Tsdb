//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Threading.Tasks;

//namespace Vibrant.Tsdb
//{
//   public class ConvertKeyConverter<TKey> : IKeyConverter<TKey>
//   {
//      public string Convert( TKey key )
//      {
//         return System.Convert.ToString( key );
//      }

//      public TKey Convert( string key )
//      {
//         return (TKey)System.Convert.ChangeType( key, typeof( TKey ) );
//      }
//   }
//}
