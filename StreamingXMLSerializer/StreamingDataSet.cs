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
        private static XmlWriterSettings xmlWriterSettings = new XmlWriterSettings()
        {
            Indent = true,
            IndentChars = "\t"
        };

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

        public List<StreamingDataTable> Tables
        {
            get
            {
                return tables;
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
            foreach (StreamingDataTable t in Tables.Distinct())
            {
                if (!schemaDataSet.Tables.Contains(t.TableName))
                    schemaDataSet.Tables.Add(t.BuildSchemaTable());
            }

            xmlWriterSettings.OmitXmlDeclaration = omitXmlDeclaration;
            using (var writer = XmlWriter.Create(file, xmlWriterSettings))
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

        /// <summary>
        /// Merges the specified dataset into this dataset. Disposes the input dataset.
        /// </summary>
        /// <param name="ds">The input StreamingDataSet.</param>
        public void Merge(StreamingDataSet ds)
        {
            foreach (var table in ds.Tables)
                this.AddTable(table);

            ds.Tables.Clear();
            ds.Dispose();
        }

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
