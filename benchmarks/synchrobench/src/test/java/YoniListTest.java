

import com.oath.oak.synchrobench.maps.MyBuffer;
import com.oath.oak.synchrobench.maps.YoniList2;
import org.junit.Test;


public class YoniListTest {

    @Test
    public void testYoniList2() throws InterruptedException {
        YoniList2<MyBuffer, MyBuffer> map = new YoniList2<>();
        int threads_num = 2;
        Thread[] threads = new Thread[threads_num];

        for (int j = 0; j < threads_num; ++j) {
            threads[j] = new Thread(() -> {
                for (int i=0; i < 1000; ++i) {
                    MyBuffer key = new MyBuffer(100);
                    MyBuffer val = new MyBuffer(100);
                    key.buffer.putInt(0,i);
                    val.buffer.putInt(0,i);
                    map.putOak(key, val);
                }
            });
        }
        for (int j = 0; j < threads_num; ++j) {
            threads[j].start();
        }
        for (int j = 0; j < threads_num; ++j) {
            threads[j].join();
        }

        System.out.println(map.size());



    }


}
