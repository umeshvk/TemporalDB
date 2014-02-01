/*******************************************************************************
 * Copyright 2014 Umesh Kanitkar
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.mvdb.etl.util;

import java.util.Date;

public class RandomUtil
{

    private static final String[] WORDS      = { "all", "the", "world", "get", "put", "set", "harry", "berry", "kerry",
            "stand", "sit", "up", "down", "above", "below", "side", "step", "song", "span", "can", "ran", "cyan",
            "red", "yellow", "hello", "world", "plan", "now", "later", "early", "late", "find", "lose", "hurry",
            "merry", "max", "min", "average", "mode", "mutiple", "fact", "fiction", "fake", "amazing", "fort", "land",
            "sea", "air", "call", "push", "pull" };

    private static final int      WORD_COUNT = WORDS.length;

    public static Date getRandomDateInRange(Date startDate, Date endDate)
    {
        long startTime = startDate.getTime();
        long endTime = endDate.getTime();
        long timeDiff = endTime - startTime;

        long delta = (long) (Math.random() * ((double) timeDiff));
        Date randomDate = new Date();
        randomDate.setTime(startTime + delta);
        return randomDate;
    }
    
    public static String getRandomString(int wordCount)
    {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < wordCount; i++)
        {
            int pos = (int) Math.floor(Math.random() * WORD_COUNT);
            sb.append(WORDS[pos]).append(" ");
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }

    public static long getRandomLong(long max)
    {
        long retval = (long) Math.floor(Math.random() * max);
        return retval;
    }

    public static long getRandomLong()
    {
        long retval = (long) Math.floor(Math.random() * Long.MAX_VALUE);
        return retval;
    }

    public static int getRandomInt(int max)
    {
        int retval = (int) Math.floor(Math.random() * max);
        return retval;
    }

    public static int getRandomInt()
    {
        int retval = (int) Math.floor(Math.random() * Integer.MAX_VALUE);
        return retval;
    }

    /**
     * @param args
     */
    public static void main(String[] args)
    {
        String str = getRandomString(10);
        System.out.println(str);
    }

}
