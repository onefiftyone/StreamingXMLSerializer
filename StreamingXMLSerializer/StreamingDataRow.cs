using System;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Text;

namespace OneFiftyOne.Serialization.StreamingXMLSerializer
{
    public class StreamingDataRow : DynamicObject
    {
        private Dictionary<string, object> _data = new Dictionary<string, object>(StringComparer.CurrentCultureIgnoreCase);

        #region properties
        public DataColumnCollection Columns { get; internal set; }

        public bool IsNull
        {
            get
            {
                return !_data.Any(kvp => kvp.Value != null);
            }
        }
        #endregion

        #region Constructors
        internal StreamingDataRow(DataColumnCollection columns)
        {
            Columns = columns;
            foreach (DataColumn c in Columns)
                _data.Add(c.ColumnName, null); 
        }

        internal StreamingDataRow(DataColumnCollection columns, IDictionary<string, object> data)
        {
            Columns = columns;
            foreach (DataColumn c in Columns)
            {
                if (!data.ContainsKey(c.ColumnName))
                    throw new Exception("Data keys do not match Column definition.");
                _data.Add(c.ColumnName, data[c.ColumnName]);
            }
        }

        #endregion

        #region Operators
        public object this[string column]
        {
            get
            {
                if (!_data.ContainsKey(column))
                    throw new KeyNotFoundException(string.Format("StreamingDataRow does not contain column: '{0}'", column));

                return _data[column];
            }
            set
            {
                if (!_data.ContainsKey(column))
                    throw new KeyNotFoundException(string.Format("StreamingDataRow does not contain column: '{0}'", column));

                _data[column] = value;
            }
        }

        public object this[DataColumn column]
        {
            get
            {
                if (!_data.ContainsKey(column.ColumnName))
                    throw new KeyNotFoundException(string.Format("StreamingDataRow does not contain column: '{0}'", column.ColumnName));

                return _data[column.ColumnName];
            }
        }

        #endregion

        #region Dynamic

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return _data.TryGetValue(binder.Name, out result);
        }

        public override IEnumerable<string> GetDynamicMemberNames()
        {
            return _data.Keys;
        }

        #endregion
    }
}
