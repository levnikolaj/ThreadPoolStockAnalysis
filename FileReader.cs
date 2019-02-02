using System;
using System.Collections.Generic;
using System.Text;
using System.IO;
using System.Threading;

namespace StockAnalysis
{
    internal class FileRangeCreator: ThreadProcRequest
    {
        internal bool firstChunk = true;
        internal long sizeOfFile;
        internal bool m_endOfFile = false;
        internal int m_numOfChunksProcessing;
        private const int MIN_CHUNK_SIZE = 10000; // 10,000
        private const int FILE_READ_BUFF_SIZE = 1000000; //1,000,000
        internal char[] buff;
        internal short dataParserObjCount = 0;
        internal int unparsedCharsInBuff = 0;
        internal StreamReader rawDataFileStream; // need to dispose of the StreamReader when at EOF
        internal string filePath;
        internal int threadsInPool;

        public FileRangeCreator()
        {
            buff = new char[FILE_READ_BUFF_SIZE];            
        }

        internal void SetFilePath(string filePath)
        {
            this.filePath = filePath;
        }

        internal void Initialize(int threads)
        {
            threadsInPool = threads;
        }

        public void OpenFile()
        {
            try
            {
                rawDataFileStream = new StreamReader(filePath);
                sizeOfFile = rawDataFileStream.BaseStream.Length;
            }
            catch(Exception e)
            {
                Program.WriteToConsole("FileReader: " + e.Message);
            }
        }

        public override void Execute(ref bool destroyAfterExecute)
        {
            int startIndex, endIndex, startIndexSkew = 0;
            int readIndex = 0;
            int charsRead = 0;
            int totalCharsInBuff;
            int buffChunkSize;
            int numOfChunks;
            
            DataChunkParser dcp;

            destroyAfterExecute = false;

            if (rawDataFileStream == null)
            {
                OpenFile();
            }
            
            // need to check if buffer is fully read
            if(unparsedCharsInBuff > 0)
            {
                // TODO: check bounds
                Array.Copy(buff, FILE_READ_BUFF_SIZE - unparsedCharsInBuff, buff, 0, unparsedCharsInBuff);
                readIndex = unparsedCharsInBuff;
            }

            charsRead = rawDataFileStream.ReadBlock(buff, readIndex, FILE_READ_BUFF_SIZE - readIndex);
            totalCharsInBuff = (charsRead + unparsedCharsInBuff);

            if(charsRead == 0)
            {
                rawDataFileStream.Dispose();
                Global.mainThreadWait.Set();
                destroyAfterExecute = true;
                return;
            }

            if(totalCharsInBuff < FILE_READ_BUFF_SIZE)
            {
                Array.Clear(buff, totalCharsInBuff, FILE_READ_BUFF_SIZE - totalCharsInBuff);
            }

            buffChunkSize = totalCharsInBuff / threadsInPool;
            
            if(buffChunkSize < MIN_CHUNK_SIZE)
            {
                buffChunkSize = MIN_CHUNK_SIZE;
            }

            numOfChunks = totalCharsInBuff / buffChunkSize;
            if((totalCharsInBuff % buffChunkSize) != 0) // if not divisible 
            {
                numOfChunks++;
            }
            Interlocked.Add(ref m_numOfChunksProcessing, numOfChunks);

            for(int i = 0; i < numOfChunks; i++)
            {
                startIndex = i * buffChunkSize + startIndexSkew;
                startIndexSkew = 0;
                endIndex = (i + 1) * buffChunkSize;

                if (endIndex > totalCharsInBuff)
                {
                    endIndex = totalCharsInBuff;
                }
                else
                {
                    // TODO: check if you need this?
                    if (endIndex >= FILE_READ_BUFF_SIZE)
                    {
                        endIndex--;
                    }
                    // TODO: look for '\r' or '\n' and then move accordingly
                    while (!buff[endIndex].Equals('\n'))
                    {
                        endIndex--;
                        startIndexSkew--;
                    }
                    startIndexSkew++;
                }
                
                if(i == numOfChunks - 1)
                {
                    unparsedCharsInBuff = totalCharsInBuff - endIndex - 1; // - 1 to get skip '\n' part infront of it
                }

                // no reference to this, DCP adds straight to final gathering place
                dcp = new DataChunkParser(startIndex, endIndex, firstChunk);
                firstChunk = false;                
                Global.threadPool.PutQueue(dcp);
            }

        }
    }

    internal class DataChunkParser: ThreadProcRequest
    {
        internal RawDataChunk parsedChunk;
        internal int rangeStart;
        internal int rangeEnd;
        internal bool globalFirstChunk = false;
        internal static int outstadingDataChunkParsers = 0;

        public DataChunkParser(int start, int end, bool firstChunk)
        {
            rangeStart = start;
            rangeEnd = end;
            globalFirstChunk = firstChunk;
            Interlocked.Add(ref outstadingDataChunkParsers, 1);
            parsedChunk = new RawDataChunk();
        }

        public override void Execute(ref bool destroyAfterExecute)
        {
            int dataChunksRemaining;
            StockDataEntry entry;
            int sectionBuffStart = rangeStart;
            int sectionBuffEnd = rangeStart;
            int reQueueRangeCreator;

            // TODO: check that rangeEnd in buffRef is a newline character
            for(int i = rangeStart; i < rangeEnd; i++)
            {
                if(Global.frp.buff[i].Equals('\r'))
                {
                    if (!globalFirstChunk)
                    {
                        // TODO: if string value in split doesn't exist, then value should be 0.
                        string dataEntrySection = new string(Global.frp.buff, sectionBuffStart, sectionBuffEnd - sectionBuffStart);
                        string[] split = dataEntrySection.Split(',');
                        entry = new StockDataEntry();
                        entry.stockCode = split[0];
                        entry.stockType = split[1];
                        entry.holderId = split[2];
                        entry.holderCountry = split[3];
                        entry.sharesHeld = Convert.ToDouble(split[4]);
                        entry.precentageSharesHeld = Convert.ToDouble(split[5]);
                        entry.direction = split[6];
                        entry.value = Convert.ToDouble(split[7]);

                        parsedChunk.chunkDataParsed.Add(entry);
                    }
                    else
                    {
                        globalFirstChunk = false;
                    }
                    sectionBuffStart = sectionBuffEnd + 2;
                }
                sectionBuffEnd++;
            }

            reQueueRangeCreator = Interlocked.Decrement(ref Global.frp.m_numOfChunksProcessing);
            if(reQueueRangeCreator == 0)
            {
                Global.threadPool.PutQueue(Global.frp);
            }
            // TODO: if m_numOfChunksProcessing = 0 then re-queue if not endOfFile
            dataChunksRemaining = Interlocked.Decrement(ref outstadingDataChunkParsers);
            if(dataChunksRemaining == 0)
            {
                //Global.mainThreadWait.Set();
            }
            Global.CombineStockDataEntryChunk(parsedChunk);
        }
    }

    internal class RawDataChunk
    {
        internal List<StockDataEntry> chunkDataParsed = new List<StockDataEntry>();
    }

    internal class StockDataEntry
    {
        internal string stockCode;
        internal string stockType; // char? 
        internal string holderId;
        internal string holderCountry;
        internal double sharesHeld;
        internal double precentageSharesHeld;
        internal string direction; // char?
        internal double value;
    }
}
