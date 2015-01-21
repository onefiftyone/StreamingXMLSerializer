using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Xml;

namespace OneFiftyOne.Serialization.StreamingXMLSerializer
{
    public class StreamingDataSet : IDisposable
    {
        //privates
        private DataSet schemaDataSet;
        private List<StreamingDataTable> tables;

        public string BaseURI { get; private set; }

        public string DataSetName
        {
            get
            {
                return (!string.IsNullOrWhiteSpace(schemaDataSet.DataSetName)) ? schemaDataSet.DataSetName : "NewDataSet";
            }
            set
            {
                schemaDataSet.DataSetName = value;
            }
        }

        public IEnumerable<StreamingDataTable> Tables
        {
            get
            {
                return tables.AsEnumerable();
            }
        }

        public StreamingDataTable this[int index]
        {
            get
            {
                return tables[index];
            }
        }

        public StreamingDataTable this[string tableName]
        {
            get
            {
                return tables.FirstOrDefault(t => t.TableName.Equals(tableName, StringComparison.CurrentCultureIgnoreCase));
            }
        }

        public StreamingDataSet()
        {
            schemaDataSet = new DataSet();
            tables = new List<StreamingDataTable>();
        }

        public StreamingDataSet(string dataSetName) : this()
        {
            DataSetName = dataSetName;
        }

        public void AddTable(StreamingDataTable table)
        {
            tables.Add(table);
        }

        #region READING

        public void ReadXML(string filename)
        {
            BaseURI = filename;
            schemaDataSet = new DataSet();
            using (XmlReader reader = XmlReader.Create(BaseURI))
            {
                reader.MoveToContent();
                if (!reader.ReadToFollowing("xs:schema"))
                    throw new InvalidOperationException("StreamingDataSet does not support XML files with missing schema information");

                using (var subReader = reader.ReadSubtree())
                {
                    schemaDataSet.ReadXmlSchema(subReader);
                    subReader.Close();
                }
                reader.Close();

                foreach (DataTable t in schemaDataSet.Tables)
                    tables.Add(new StreamingDataTable(BaseURI, t));
            }
        }

        #endregion

        #region WRITING

        public void WriteXML(string file, bool omitXmlDeclaration = true)
        {
            if (schemaDataSet == null)
                throw new Exception("DataSet is not initialized");

            //build schema for contained tables
            schemaDataSet.Tables.Clear();
            foreach (StreamingDataTable t in Tables)
                schemaDataSet.Tables.Add(t.BuildSchemaTable());

            using (var writer = XmlWriter.Create(file, new XmlWriterSettings { OmitXmlDeclaration = omitXmlDeclaration }))
            {
                //write root element
                writer.WriteStartElement(this.DataSetName);

                //write schema
                schemaDataSet.WriteXmlSchema(writer);

                //write contents
                foreach (StreamingDataTable t in Tables)
                    t.WriteXML(writer);

                //write end root element
                writer.WriteEndElement();
            }
        }

        #endregion

        #region IDisposable Members

        public void Dispose()
        {
            if (Tables != null)
            {
                foreach (var table in Tables)
                    table.Dispose();
            }

            schemaDataSet.Dispose();
        }

        #endregion
    }
}
