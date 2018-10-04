/**
 * Copyright 2014-2016 yangming.liu<bytefox@126.com>.
 *
 * This copyrighted material is made available to anyone wishing to use, modify,
 * copy, or redistribute it subject to the terms and conditions of the GNU
 * Lesser General Public License, as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this distribution; if not, see <http://www.gnu.org/licenses/>.
 */
package org.bytesoft.bytetcc.work;

import javax.resource.spi.work.Work;

import org.bytesoft.compensable.CompensableBeanFactory;
import org.bytesoft.compensable.aware.CompensableBeanFactoryAware;
import org.bytesoft.transaction.TransactionRecovery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CompensableWork implements Work, CompensableBeanFactoryAware {
	static final Logger logger = LoggerFactory.getLogger(CompensableWork.class);

	static final long SECOND_MILLIS = 1000L;
	private long stopTimeMillis = -1;
	private long delayOfStoping = SECOND_MILLIS * 15;
	private long recoveryInterval = SECOND_MILLIS * 60;

	private boolean initialized = false;

	@javax.inject.Inject
	private CompensableBeanFactory beanFactory;

	/**
	 * TODO CompensableWork 事务恢复
	 * 在服务启动的时候，bytetcc框架会通过后台线程启动一个task，CompensableWork，这个里面会启动一个事务恢复的这么一个过程，
	 * 如果一个事务执行一半儿，系统崩溃了。系统重启的时候，会有一个事务恢复的过程，也就是说从数据库、日志文件里加载出来没完成的事务的状态，
	 * 然后继续去执行这个事务
	 */

	private void initializeIfNecessary() {
		TransactionRecovery compensableRecovery = this.beanFactory.getCompensableRecovery();
		if (this.initialized == false) {
			try {
				compensableRecovery.startRecovery();//TODO 开始执行事务恢复
				this.initialized = true;
				compensableRecovery.timingRecover();
			} catch (RuntimeException rex) {
				logger.error("Error occurred while initializing the compensable work.", rex);
			}
		}
	}

	public void run() {
		TransactionRecovery compensableRecovery = this.beanFactory.getCompensableRecovery();

		this.initializeIfNecessary();

		long nextRecoveryTime = 0;
		while (this.currentActive()) {

			this.initializeIfNecessary();

			long current = System.currentTimeMillis();
			if (current >= nextRecoveryTime) {
				nextRecoveryTime = current + this.recoveryInterval;//60s

				try {
					compensableRecovery.timingRecover();
				} catch (RuntimeException rex) {
					logger.error(rex.getMessage(), rex);
				}
			}

			this.waitForMillis(100L);

		} // end-while (this.currentActive())
	}

	private void waitForMillis(long millis) {
		try {
			Thread.sleep(millis);
		} catch (Exception ignore) {
			logger.debug(ignore.getMessage(), ignore);
		}
	}

	public void release() {
		this.stopTimeMillis = System.currentTimeMillis() + this.delayOfStoping;
	}

	protected boolean currentActive() {
		return this.stopTimeMillis <= 0 || System.currentTimeMillis() < this.stopTimeMillis;
	}

	public long getDelayOfStoping() {
		return delayOfStoping;
	}

	public long getRecoveryInterval() {
		return recoveryInterval;
	}

	public void setRecoveryInterval(long recoveryInterval) {
		this.recoveryInterval = recoveryInterval;
	}

	public void setDelayOfStoping(long delayOfStoping) {
		this.delayOfStoping = delayOfStoping;
	}

	public void setBeanFactory(CompensableBeanFactory tbf) {
		this.beanFactory = tbf;
	}

}
