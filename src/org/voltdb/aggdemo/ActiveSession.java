/*
 * Copyright (C) 2025 Volt Active Data Inc.
 *
 * Use of this source code is governed by an MIT
 * license that can be found in the LICENSE file or at
 * https://opensource.org/licenses/MIT.
 */

package org.voltdb.aggdemo;

public class ActiveSession {

    int number;

    int remainingActvity;

    public ActiveSession(int number, int remainingActvity) {
        super();
        this.number = number;
        this.remainingActvity = remainingActvity;
    }

    /**
     * @return the remainingActvity
     */
    public int getAndDecrementRemainingActvity() {
        return remainingActvity--;
    }

    public int getNumber() {
       
        return number;
    }

}
