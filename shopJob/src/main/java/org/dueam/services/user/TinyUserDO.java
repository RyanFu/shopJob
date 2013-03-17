package org.dueam.services.user;

import java.io.Serializable;
import java.util.Date;

/**
 * 原子用户DO
 */
public class TinyUserDO extends VersionDO implements Serializable{

	private static final long serialVersionUID = -5624626909144351763L;
	/**
	 * 会员Id ID
	 */
	protected String id;
	/**
	 * 登录名/昵称 NICK
	 */
	protected String nick;
	/**
	 * 电子邮件 EMAIL 注册和修改的时候会进行唯一性检查。
	 */
	protected String email;
	/**
	 * USER_ID 论坛、session中使用ID 用户的数字ID，序列生成
	 */
	protected Long userId;
	
	/**
	 * 用户上次访问时间，这个对应 <code>bmw_users_extra.lastvisit</code>
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