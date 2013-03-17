package org.dueam.services.user;

import java.io.Serializable;
import java.util.Date;

/**
 * ԭ���û�DO
 */
public class TinyUserDO extends VersionDO implements Serializable{

	private static final long serialVersionUID = -5624626909144351763L;
	/**
	 * ��ԱId ID
	 */
	protected String id;
	/**
	 * ��¼��/�ǳ� NICK
	 */
	protected String nick;
	/**
	 * �����ʼ� EMAIL ע����޸ĵ�ʱ������Ψһ�Լ�顣
	 */
	protected String email;
	/**
	 * USER_ID ��̳��session��ʹ��ID �û�������ID����������
	 */
	protected Long userId;
	
	/**
	 * �û��ϴη���ʱ�䣬�����Ӧ <code>bmw_users_extra.lastvisit</code>
	 */
	protected Date lastVisit;

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getNick() {
		return nick;
	}

	public void setNick(String nick) {
		this.nick = nick;
	}

	public Long getUserId() {
		return userId;
	}

	public void setUserId(Long userId) {
		this.userId = userId;
	}

	public Date getLastVisit() {
		return lastVisit;
	}

	public void setLastVisit(Date lastVisit) {
		this.lastVisit = lastVisit;
	}
	
}