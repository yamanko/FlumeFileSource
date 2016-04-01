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
 * tail��־�ļ���flume source�����ݶ���õ�ʱ��������ȡָ�����ļ����ݣ�ͬʱ����ǰ��ȡ�ļ���ʱ������α�־û����ļ�ϵͳcursor.pt
 * ������־�ļ����е�ʱ��ѡ��ǿ�й����̡߳�
 * @author yeminke
 * @version $Id: TailFileSource.java, v 0.1 2015��10��14�� ����5:34:34 yeminke Exp $
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
     * RandomAccessFileĬ�ϲ���ISO-8859-1�����ȡ�ļ�
     */
    private static final String     inCoding             = "ISO-8859-1";
    /**
     * Hadoop��Ⱥ���õı��뷽ʽ
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
                    //logger.info("��ǰsource counterΪ�� " + sourceCounter.getAppendAcceptedCount());
                }
            }

            @Override
            public void newFileLine(Event event) {
                getChannelProcessor().processEvent(event);
                sourceCounter.incrementAppendAcceptedCount();
                logger.info("��ǰsource counterΪ�� " + sourceCounter.getAppendAcceptedCount());
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
            logger.info("��ʼ����TailFileSource�߳�");
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
                         * ��һ����������������last modifiedʱ��Ϊ��ǰ�ļ���ʱ�䣬����ζ������������֮ǰ����־�ļ������ᱻ����
                         */
                        cursor.set(Long.parseLong(ps
                            .getProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION)));
                        ps.setProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG,
                            String.valueOf(tf.lastModified()));
                    } else {
                        /**
                         * �ϵ�����������ֻ�������α�
                         */
                        cursor.set(Long.parseLong(ps
                            .getProperty(CursorRepositorInstance.LAST_PROCESSED_POSITION)));
                    }
                    logger
                        .info("***��ǰ�ļ�λ��Ϊ***�� "
                              + ps.getProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG)
                              + " with "
                              + dateFormat.format(new Date(Long.parseLong(ps
                                  .getProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG)))));
                    logger.info("***��ǰ�ļ��α�λ��Ϊ***�� " + cursor.get());
                    flag = false;
                } catch (Exception e) {
                    try {
                        logger.error("����  - " + e.getMessage() + ",��������:" + tailFile);
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
                         * ��ȡ��ǰ���ں����һ����־���������
                         */
                        boolean isBreakFromProcessing = false;
                        Date lastTimeDate = new Date(Long.parseLong(ps.getProperty(String
                            .valueOf(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG))));
                        Date newTimeDate = new Date(new File(tailFile).lastModified());
                        logger.info("��־����޸�ʱ��Ϊ��" + dateFormat.format(newTimeDate) + "[" + tailFile
                                    + "]");
                        logger.info("���һ����־����ʱ��Ϊ��" + dateFormat.format(lastTimeDate) + "["
                                    + tailFile + "]");
                        if (!isSameYearMonthDay(lastTimeDate, newTimeDate)) {
                            logger.info("[LogDayCut]��־������ ");
                            logger.info("[LogDayCut]��־����޸�ʱ��Ϊ " + dateFormat.format(newTimeDate) + "[" + tailFile + "]");
                            logger
                                .info("[LogDayCut]���һ����־����ʱ��Ϊ��" + dateFormat.format(lastTimeDate) + "[" + tailFile + "]");
                            processDayCutLogs(lastTimeDate, newTimeDate);
                            File tf = new File(tailFile);
                            file = new RandomAccessFile(tf, "r");
                            ps.setProperty(CursorRepositorInstance.LAST_MODIFIED_TIME_LONG,
                                String.valueOf(tf.lastModified()));
                            cursor.set(0);
                        }

                        long fileLength = file.length();
                        if (fileLength > cursor.get()) {
                            logger.info("��ǰ�ļ�����Ϊ�� " + fileLength + "[" + tailFile + "]");
                            logger.info("��ǰ�ļ��α�λ��Ϊ�� " + cursor.get() + "[" + tailFile + "]");
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
                                     * �����ǰlogʱ�佫����ҹ����ǿ��break�����̹߳��𣬾��������ļ�
                                     */
                                    Date fDate = new Date(System.currentTimeMillis());
                                    if (fDate.getHours() == 23 && fDate.getMinutes() == 59
                                        && 0 <= fDate.getSeconds() && 59 >= fDate.getSeconds()) {
                                        logger.info("��ҹ��������������");
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
                            logger.info("��ҹ���꣬��Ϣ" + midInterval + "ms");
                            Thread.sleep(midInterval);
                        }

                        else {
                            logger.info("���ˣ���Ϣ" + collectInterval + "ms");
                            Thread.sleep(collectInterval);
                        }

                    } catch (Exception e) {
                        logger.error("��ȡ�ʹ�����־����", e);
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
         * ���������������δ���������־����
         * δ�����ļ�����ʽΪfront-page-digest.log.yyyy-MM-dd
         * @param lastTimeDate
         * @param newTimeDate
         */

        private void processDayCutLogs(Date lastTimeDate, Date newTimeDate) {
            List<Date> lapDates = getLapDays(lastTimeDate, newTimeDate);
            logger.info("[LogDayCut]����δ�����ļ�����Ϊ��" + lapDates.size());
            for (Date d : lapDates) {
                logger.info("[LogDayCut]��ʼ���������־�ļ���" + tailFile + "." + fileFormat.format(d));
                File pFile = new File(tailFile + "." + fileFormat.format(d));
                /**
                 * ���������ϴδ�����ļ������α�λ��Ϊcursor��ǰ��������־�ļ�����ͷ��ʼ����
                 */
                if (isSameYearMonthDay(d, lastTimeDate))
                    processLogsPerFile(pFile, cursor.get());
                else
                    processLogsPerFile(pFile, 0);
            }

        }

        /**
         * ��������ϴδ���Ŀǰδ���������
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
            logger.info("��������־�ļ���" + pFile.getAbsolutePath() + " starting on position " + position);
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
                logger.error("���������־�ļ��ǳ���", e);
            } finally {
                try {
                    if (file != null)
                        file.close();
                } catch (Exception e) {
                    //��ʱ����
                }
            }
        }

        private Properties readPointerFile() {
            logger.info("��ȡ�α��ļ�:" + pointerFile);
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
