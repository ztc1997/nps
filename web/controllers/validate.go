package controllers

import (
	"regexp"
	"strings"
)

var haGroupNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// validateHAGroup 校验高可用组名。required 为 true 时组名不能为空。
func validateHAGroup(name string, required bool) string {
	name = strings.TrimSpace(name)
	if name == "" {
		if required {
			return "client group is required"
		}
		return ""
	}
	if !haGroupNameRE.MatchString(name) {
		return "client group allows only letters, digits, _ and - (max 32 chars)"
	}
	return ""
}

// validateClientWebPair 校验客户端 Web 登录用户名和密码必须成对设置。
func validateClientWebPair(webUserName, webPassword string) string {
	hasUserName := strings.TrimSpace(webUserName) != ""
	hasPassword := strings.TrimSpace(webPassword) != ""
	if hasUserName != hasPassword {
		return "web username and password must be set together"
	}
	return ""
}

// validateTunnelInput 按隧道模式校验必填项。
func validateTunnelInput(mode, target, password, localPath string) string {
	switch mode {
	case "tcp", "udp":
		if strings.TrimSpace(target) == "" {
			return "target is required"
		}
	case "secret", "p2p":
		if strings.TrimSpace(target) == "" {
			return "target is required"
		}
		if strings.TrimSpace(password) == "" {
			return "password is required"
		}
	case "file":
		if strings.TrimSpace(localPath) == "" {
			return "local path is required"
		}
	case "socks5", "httpProxy":
	default:
		return "invalid tunnel type"
	}
	return ""
}

// validateHostInput 校验域名解析必填项。
func validateHostInput(host, target string) string {
	if strings.TrimSpace(host) == "" {
		return "host is required"
	}
	if strings.TrimSpace(target) == "" {
		return "target is required"
	}
	return ""
}
