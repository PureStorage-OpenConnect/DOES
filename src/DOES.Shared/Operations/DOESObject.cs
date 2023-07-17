using System;
using DOES.Shared.Debug;
using System.Collections.Generic;

namespace DOES.Shared.Operations
{
    [Serializable()]
    public class DOESObject_AnalyticsGroupView
   {
        private List<DOESObject_AnalitcsView> _groupObjects;

        public DOESObject_AnalyticsGroupView()
        {
            _groupObjects = new List<DOESObject_AnalitcsView>();
        }

        public void AddGroupSet(DOESObject_AnalitcsView objectValues)
        {
            _groupObjects.Add(objectValues);
        }


   }
    [Serializable()]
    public class DOESObject_AnalitcsView
    {
        private List<float> _valuesList;
        private List<DateTime> _timeStampList;

        public DOESObject_AnalitcsView(int objectID)
        {
            ObjectID = objectID;
            _valuesList = new List<float>();
            _timeStampList = new List<DateTime>();
        }

        public int ObjectID { get; }
        
        public void AddValueSet(float value, DateTime timeStamp)
        {
            _valuesList.Add(value);
            _timeStampList.Add(timeStamp);
        }
    }

    public class DOESObject
    {
        private DOESTest _test;
        private MessageQueue _queue;
        private List<int> _objectIDs = new List<int>();


        public DOESObject(DOESTest test, MessageQueue queue)
        {
            _test = test;
            _queue = queue;
        }

        public DOESObject(DOESTest test, string objectTag, MessageQueue queue)
        {
            _test = test;
            ObjectTag = objectTag;
            _queue = queue;
        }

        public DOESObject(DOESTest test, int objectID, string objectTag, MessageQueue queue)
        {
            _test = test;
            ObjectID = objectID;
            ObjectTag = objectTag;
            _queue = queue;
        }

        public DOESObject(DOESTest test, string objectTag, string Category, MessageQueue queue)
        {
            _test = test;
            ObjectTag = objectTag;
            ObjectCategory = Category;
            _queue = queue;
        }

        public DOESObject(DOESTest test, int objectID,  string objectTag, string Category, MessageQueue queue)
        {
            _test = test;
            ObjectID = objectID;
            ObjectTag = objectTag;
            ObjectCategory = Category;
            _queue = queue;
        }

        public void AddObjectID(int objectID)
        {
            _objectIDs.Add(objectID);
        }

        public List<int> RetrieveObjectIDs()
        {
            return _objectIDs;
        }

        public int ObjectIDCount()
        {
            return _objectIDs.Count;
        }

        public int ObjectID { get; set; }
        public string ObjectTag { get; set; }
        public string ObjectCategory { get; set; }
        public DOESTest Test { get { return _test; } }
    }
}
