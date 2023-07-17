using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace DOES.Shared.Debug
{
    /// <summary>
    /// This class must be inherited by all vendor data object implementations. 
    /// </summary>
    public class Message
    {
        private DateTime _messageTime;
        private string _message;
        public enum MessageType
        {
            Report,
            Info,
            Warning,
            Error,
            Command
        };

        /// <summary>
        /// This class must be inherited by all vendor data object implementations. 
        /// </summary>
        private MessageType _messageType;

        /// <summary>
        /// This class must be inherited by all vendor data object implementations. 
        /// </summary>
        public Message(DateTime time, string message, MessageType messageType)
        {
            _messageTime = time;
            _message = message;
            _messageType = messageType;
        }

        /// <summary>
        /// This class must be inherited by all vendor data object implementations. 
        /// </summary>
        public Tuple<MessageType, string> GetFormattedMessage()
        {
            string formattedText = _messageType.ToString() + " : " + _messageTime.ToString() + "   " + _message;
            return Tuple.Create(_messageType, formattedText);
        }
    }

    /// <summary>
    /// This class must be inherited by all vendor data object implementations. 
    /// </summary>
    public class MessageQueue
    {
        private BlockingCollection<Message> _messageQueue = new BlockingCollection<Message>(new ConcurrentQueue<Message>());

        /// <summary>
        /// This class must be inherited by all vendor data object implementations. 
        /// </summary>
        public void AddMessage(Message message)
        {
            _messageQueue.TryAdd(message, 1000);
        }

        /// <summary>
        /// This class must be inherited by all vendor data object implementations. 
        /// </summary>
        public List<Message> GetAllMessages()
        {
            List<Message> messages = new List<Message>();
            while (_messageQueue.Count != 0)
            {
                messages.Add(_messageQueue.Take());
            }
            return messages;
        }
    }
}

