package com.yylc.monitor.flume.source;

import java.io.File;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.apache.log4j.Logger;

import com.yylc.monitor.flume.health.ServerHealthStatusManager;
import com.yylc.monitor.flume.source.util.CursorRepositor;
import com.yylc.monitor.flume.source.util.CursorRepositorInstance;

/**
 * tail日志文件的flume source，根据定义好的时间间隔，读取指定的文件内容，同时将当前读取文件的时间戳和游标持久化到文件系统cursor.pt
 * 处理日志文件日切的时候，选择强行挂起线程。
 * @author yeminke
 * @version $Id: TailFileSource.java, v 0.1 2015年10月14日 下午5:34:34 yeminke Exp $
 */
public class TailFileSource extends AbstractSource implements EventDrivenSource, Configurable {
    private static final Logger     logger               = Logger.getLogger(TailFileSource.class);
    /**
     */
    private SourceCounter           sourceCounter;
    private String                  pointerFile;
    private String                  tailFile;
    private long                    collectInterval;
    private int                     batchLine;
    private boolean                 batch;
    private AtomicBoolean           tailRun              = new AtomicBoolean(true);
    private AtomicLong              cursor               = new AtomicLong(0);
    private CursorRepositor         cursorRepositor;
    private static SimpleDateFormat dateFormat           = new SimpleDateFormat(
                                                             "yyyy-MM-dd HH:mm:ss,SSS");
    private static SimpleDateFormat fileFormat           = new SimpleDateFormat("yyyy-MM-dd");
    public final static long        ONE_DAY_MILL_SECONDS = 86400000;

    /**
     * RandomAccessFile默认采用ISO-8859-1解码读取文件
     */
    private static final String     inCoding             = "ISO-8859-1";
    /**
     * Hadoop集群采用的编码方式
     */
    private static final String     outCoding            = "UTF-8";
    private static long             midInterval          = 90000L;

    @Override
    public void configure(Context context) {
        if (sourceCounter == null) {
            sourceCounter = new SourceCounter(getName());
        }
        pointerFile = context.getString("tailfile.pointer", "cursor.pt");
        tailFile = context.getString("tailfile.file", "front_page_digest.log");
        collectInterval = context.getLong("tailfile.collectInterval", 60000L);
        batchLine = context.getInteger("tailfile.batchLine", 10);
        batch = context.getBoolean("tailfile.batch", false);
        cursorRepositor = new CursorRepositor(pointerFile);
        cursorRepositor.createIfAbsent();
    }

    @Override
    public synchronized void start() {
        super.start();
        sourceCounter.start();
        TailFile tf = new TailFile();
        tf.addFileTailerListener(new FileTailerListener() {
            @Override
            public void newFileLine(List<Event> events) {
                if (!events.isEmpty()) {
                    getChannelProcessor().processEventBatch(events);
                    sourceCounter.incrementAppendBatchAcceptedCount();
                    sourceCounter.addToEventAcceptedCount(events.size());
                    //logger.info("当前source counter为： " + sourceCounter.getAppendAcceptedCount());
                }
            }

            @Override
            public void newFileLine(Event event) {
                getChannelProcessor().processEvent(event);
                sourceCounter.incrementAppendAcceptedCount();
                logger.info("当前source counter为： " + sourceCounter.getAppendAcceptedCount());
            }
        });
        Thread t = new java.lang.Thread(tf);
        t.setDaemon(true);
        t.start();
    }

    @Override
    public synchronized void stop() {
        tailRun.set(false);
        super.stop();
        sourceCounter.stop();
    }

    protected interface FileTailerListener {
        public void newFileLine(List<Event> events);

        public void newFileLine(Event event);
    }

    protected class TailFile implements java.lang.Runnable {
        private Set<FileTailerListener> listeners = new HashSet<FileTailerListener>();

        public void addFileTailerListener(FileTailerListener l) {
            this.listeners.add(l);
        }

        public void removeFileTailerListener(FileTailerListener l) {
            this.listeners.remove(l);
        }

        @Override
        public void run() {
            logger.info("开始运行TailFileSource线程");
            Properties ps = this.readPointerFile();
            logger.info("[properties] "
                        + ps.getProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG));
            logger.info("[properties] "
                        + ps.getProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION));
            RandomAccessFile file = null;
            boolean flag = true;
            while (flag) {
                try {
                    File tf = new File(tailFile);
                    file = new RandomAccessFile(tf, "r");

                    if (Long.parseLong(ps
                        .getProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG)) == tf
                        .lastModified()) {
                        cursor.set(Long.parseLong(ps
                            .getProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION)));
                    } else if (ps.getProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG)
                        .equalsIgnoreCase("0")) {
                        /**
                         * 第一次正常启动，设置last modified时间为当前文件的时间，这意味着在启动当日之前的日志文件将不会被处理
                         */
                        cursor.set(Long.parseLong(ps
                            .getProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION)));
                        ps.setProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG,
                            String.valueOf(tf.lastModified()));
                    } else {
                        /**
                         * 断点后继续启动，只需设置游标
                         */
                        cursor.set(Long.parseLong(ps
                            .getProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION)));
                    }
                    logger
                        .info("***当前文件位置为***： "
                              + ps.getProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG)
                              + " with "
                              + dateFormat.format(new Date(Long.parseLong(ps
                                  .getProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG)))));
                    logger.info("***当前文件游标位置为***： " + cursor.get());
                    flag = false;
                } catch (Exception e) {
                    try {
                        logger.error("出错  - " + e.getMessage() + ",继续尝试:" + tailFile);
                        Thread.sleep(5000);
                    } catch (Exception e1) {

                    }
                }
            }

            while (tailRun.get()) {
                float load = ServerHealthStatusManager.isLoadTooHigh();
                logger.info("current cpu load for this box is:" + load);
                if (!(ServerHealthStatusManager.isLoadTooHigh() > 0f)) {
                    try {

                        /**
                         * 获取当前日期和最近一次日志处理的日期
                         */
                        boolean isBreakFromProcessing = false;
                        Date lastTimeDate = new Date(Long.parseLong(ps.getProperty(String
                            .valueOf(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG))));
                        Date newTimeDate = new Date(new File(tailFile).lastModified());
                        logger.info("日志最后修改时间为：" + dateFormat.format(newTimeDate) + "[" + tailFile
                                    + "]");
                        logger.info("最近一次日志处理时间为：" + dateFormat.format(lastTimeDate) + "["
                                    + tailFile + "]");
                        if (!isSameYearMonthDay(lastTimeDate, newTimeDate)) {
                            logger.info("[LogDayCut]日志被日切 ");
                            logger.info("[LogDayCut]日志最后修改时间为 " + dateFormat.format(newTimeDate) + "[" + tailFile + "]");
                            logger
                                .info("[LogDayCut]最近一次日志处理时间为：" + dateFormat.format(lastTimeDate) + "[" + tailFile + "]");
                            processDayCutLogs(lastTimeDate, newTimeDate);
                            File tf = new File(tailFile);
                            file = new RandomAccessFile(tf, "r");
                            ps.setProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG,
                                String.valueOf(tf.lastModified()));
                            cursor.set(0);
                        }

                        long fileLength = file.length();
                        if (fileLength > cursor.get()) {
                            logger.info("当前文件长度为： " + fileLength + "[" + tailFile + "]");
                            logger.info("当前文件游标位置为： " + cursor.get() + "[" + tailFile + "]");
                            file.seek(cursor.get());
                            String line = file.readLine();
                            int i = 1;
                            if (batch) {
                                java.util.List<Event> batchAl = new java.util.ArrayList<Event>(
                                    batchLine);
                                while (line != null) {
                                    batchAl.add(EventBuilder.withBody(ISO8859ToUTF(line).getBytes(
                                        outCoding)));
                                    /**
                                     * 如果当前log时间将近午夜，则强行break，让线程挂起，静候日切文件
                                     */
                                    Date fDate = new Date(System.currentTimeMillis());
                                    if (fDate.getHours() == 23 && fDate.getMinutes() == 59
                                        && 0 <= fDate.getSeconds() && 59 >= fDate.getSeconds()) {
                                        logger.info("午夜将至，静候日切");
                                        isBreakFromProcessing = true;
                                        break;
                                    }

                                    if (i % batchLine == 0) {
                                        fireNewFileLine(batchAl);
                                        batchAl.clear();
                                        cursor.set(file.getFilePointer());
                                        ps.setProperty(
                                            CursorRepositorInstance.LAST_PROCESSED_POSITION,
                                            String.valueOf(cursor.get()));
                                        ps.setProperty(
                                            CursorRepositorInstance.LAST_MODIFIED_TIME_LONG,
                                            String.valueOf(newTimeDate.getTime()));
                                        writePointerFile(ps);
                                    }
                                    line = file.readLine();
                                    i++;
                                }

                                if (!batchAl.isEmpty()) {
                                    fireNewFileLine(batchAl);
                                    batchAl.clear();
                                    cursor.set(file.getFilePointer());
                                    ps.setProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION,
                                        String.valueOf(cursor.get()));
                                    ps.setProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG,
                                        String.valueOf(newTimeDate.getTime()));
                                    writePointerFile(ps);
                                }
                            } else {
                                while (line != null) {
                                    fireNewFileLine(EventBuilder.withBody(ISO8859ToUTF(line)
                                        .getBytes(outCoding)));
                                    line = file.readLine();
                                }
                                cursor.set(file.getFilePointer());
                                ps.setProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION,
                                    String.valueOf(cursor.get()));
                                ps.setProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG,
                                    String.valueOf(newTimeDate.getTime()));
                                writePointerFile(ps);
                            }

                        }

                        if (isBreakFromProcessing) {
                            logger.info("午夜惊魂，休息" + midInterval + "ms");
                            Thread.sleep(midInterval);
                        }

                        else {
                            logger.info("累了，休息" + collectInterval + "ms");
                            Thread.sleep(collectInterval);
                        }

                    } catch (Exception e) {
                        logger.error("读取和处理日志出错：", e);
                    }
                }

            }

            try {
                if (file != null)
                    file.close();
            } catch (Exception e) {

            }
        }

        /**
         * 处理因日切引起的未被处理的日志内容
         * 未处理文件名格式为front-page-digest.log.yyyy-MM-dd
         * @param lastTimeDate
         * @param newTimeDate
         */

        private void processDayCutLogs(Date lastTimeDate, Date newTimeDate) {
            List<Date> lapDates = getLapDays(lastTimeDate, newTimeDate);
            logger.info("[LogDayCut]过期未处理文件个数为：" + lapDates.size());
            for (Date d : lapDates) {
                logger.info("[LogDayCut]开始处理过期日志文件：" + tailFile + "." + fileFormat.format(d));
                File pFile = new File(tailFile + "." + fileFormat.format(d));
                /**
                 * 如果是最近上次处理的文件，则游标位置为cursor当前，否则日志文件都从头开始处理
                 */
                if (isSameYearMonthDay(d, lastTimeDate))
                    processLogsPerFile(pFile, cursor.get());
                else
                    processLogsPerFile(pFile, 0);
            }

        }

        /**
         * 计算距离上次处理到目前未处理的日期
         * 
         * @param oldTime
         * @param newTimeDate
         * @return
         */

        private List<Date> getLapDays(Date oldTime, Date newTimeDate) {
            List<Date> result = new ArrayList<Date>();
            int i = 0;
            while (!isSameYearMonthDay(getAfterDate(oldTime, i), newTimeDate)) {
                result.add(getAfterDate(oldTime, i));
                i++;
            }
            return result;
        }

        public Date getAfterDate(Date dayPosition, int days) {
            return new Date(dayPosition.getTime() + (ONE_DAY_MILL_SECONDS * days));
        }

        public long getDiffDays(Date one, Date two) {
            Calendar sysDate = new GregorianCalendar();
            sysDate.setTime(one);
            Calendar failDate = new GregorianCalendar();
            failDate.setTime(two);
            return (sysDate.getTimeInMillis() - failDate.getTimeInMillis())
                   / (ONE_DAY_MILL_SECONDS);
        }

        public boolean isSameYearMonthDay(Date lastTimeDate, Date newTimeDate) {
            return (lastTimeDate.getYear() == newTimeDate.getYear()
                    && lastTimeDate.getMonth() == newTimeDate.getMonth() && lastTimeDate.getDate() == newTimeDate
                .getDate());
        }

        private void processLogsPerFile(File pFile, long position) {
            logger.info("处理老日志文件：" + pFile.getAbsolutePath() + " starting on position " + position);
            RandomAccessFile file = null;
            try {
                file = new RandomAccessFile(pFile, "r");
                file.seek(position);
                String line = file.readLine();
                int i = 1;
                long fileLength = file.length();
                if (fileLength > cursor.get()) {
                    if (batch) {
                        java.util.List<Event> batchAl = new java.util.ArrayList<Event>(batchLine);
                        while (line != null) {
                            batchAl.add(EventBuilder.withBody(ISO8859ToUTF(line)
                                .getBytes(outCoding)));
                            if (i % batchLine == 0) {
                                fireNewFileLine(batchAl);
                                batchAl.clear();
                            }
                            line = file.readLine();
                            i++;
                        }

                        if (!batchAl.isEmpty()) {
                            fireNewFileLine(batchAl);
                            batchAl.clear();
                        }
                    } else {
                        while (line != null) {
                            fireNewFileLine(EventBuilder.withBody(ISO8859ToUTF(line).getBytes(
                                outCoding)));
                            line = file.readLine();
                        }
                    }
                }

            } catch (Exception e) {
                logger.error("处理过期日志文件是出错：", e);
            } finally {
                try {
                    if (file != null)
                        file.close();
                } catch (Exception e) {
                    //暂时忽略
                }
            }
        }

        private Properties readPointerFile() {
            logger.info("读取游标文件:" + pointerFile);
            return cursorRepositor.getCurrentPosition();
        }

        private void writePointerFile(Properties ps) {
            cursorRepositor.updateCursor(
                ps.getProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG),
                ps.getProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION));

        }

        protected void fireNewFileLine(List<Event> events) {
            for (Iterator<FileTailerListener> i = this.listeners.iterator(); i.hasNext();) {
                FileTailerListener l = i.next();
                l.newFileLine(events);
            }
        }

        protected void fireNewFileLine(Event event) {
            for (Iterator<FileTailerListener> i = this.listeners.iterator(); i.hasNext();) {
                FileTailerListener l = i.next();
                l.newFileLine(event);
            }
        }

        /**
         * convert ISO8859-1 to UTF8 since RandonAccessFile read files by ISO-8859-1 while Hadoop system use UTF8
         * 
         * @param source
         * @return
         */
        protected String ISO8859ToUTF(String source) {
            String target = null;
            try {
                target = new String(source.getBytes(inCoding), outCoding);
            } catch (UnsupportedEncodingException e) {
                logger.error("", e);
            }
            return target;
        }
    }
}
