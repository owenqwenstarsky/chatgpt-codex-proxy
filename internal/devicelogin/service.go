package devicelogin

import (
	"context"
	"strings"
	"sync"
	"time"

	"chatgpt-codex-proxy/internal/accounts"
	"chatgpt-codex-proxy/internal/codexauth"
)

type DeviceLoginService struct {
	mu       sync.RWMutex
	oauth    *codexauth.OAuthService
	accounts *accounts.Service
	timeout  time.Duration
	logins   map[string]*pendingLogin
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	closed   bool
}

type pendingLogin struct {
	DeviceLoginRecord
	DeviceAuthID string
	Interval     time.Duration
}

func NewDeviceLoginService(oauth *codexauth.OAuthService, accountsSvc *accounts.Service, timeout time.Duration) *DeviceLoginService {
	ctx, cancel := context.WithCancel(context.Background())
	return &DeviceLoginService{
		oauth:    oauth,
		accounts: accountsSvc,
		timeout:  timeout,
		logins:   make(map[string]*pendingLogin),
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (s *DeviceLoginService) Start(ctx context.Context) (DeviceLoginRecord, error) {
	resp, err := s.oauth.RequestDeviceCode(ctx)
	if err != nil {
		return DeviceLoginRecord{}, err
	}

	now := time.Now().UTC()
	login := &pendingLogin{
		DeviceLoginRecord: DeviceLoginRecord{
			LoginID:   "login_" + now.Format("20060102150405.000000000"),
			AuthURL:   s.oauth.DeviceAuthURL(),
			UserCode:  resp.UserCode,
			Status:    DeviceLoginPending,
			CreatedAt: now,
			ExpiresAt: now.Add(s.timeout),
		},
		DeviceAuthID: resp.DeviceAuthID,
		Interval:     time.Duration(max(resp.Interval, 5)) * time.Second,
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return DeviceLoginRecord{}, context.Canceled
	}
	s.logins[login.LoginID] = login
	s.wg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.wg.Done()
		s.poll(login)
	}()

	return login.DeviceLoginRecord, nil
}

func (s *DeviceLoginService) Get(loginID string) (DeviceLoginRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	login, ok := s.logins[loginID]
	if !ok {
		return DeviceLoginRecord{}, false
	}
	return login.DeviceLoginRecord, true
}

func (s *DeviceLoginService) poll(login *pendingLogin) {
	ctx, cancel := context.WithDeadline(s.ctx, login.ExpiresAt)
	defer cancel()

	ticker := time.NewTicker(login.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.setStatus(login.LoginID, DeviceLoginPending, DeviceLoginExpired, "device login expired")
			return
		case <-ticker.C:
			result, err := s.oauth.PollDeviceCode(ctx, login.DeviceAuthID, login.UserCode)
			if err != nil {
				text := strings.ToLower(err.Error())
				if strings.Contains(text, "authorization_pending") || strings.Contains(text, "not found") {
					continue
				}
				s.setStatus(login.LoginID, "", DeviceLoginError, err.Error())
				return
			}

			if result == nil {
				continue
			}

			token, accountID, err := s.oauth.ExchangeAuthorizationCode(ctx, result.AuthorizationCode, result.CodeVerifier)
			if err != nil {
				s.setStatus(login.LoginID, "", DeviceLoginError, err.Error())
				return
			}
			if strings.TrimSpace(accountID) == "" {
				s.setStatus(login.LoginID, "", DeviceLoginError, "oauth exchange did not return account_id")
				return
			}

			if _, err := s.accounts.UpsertFromToken(accountID, token); err != nil {
				s.setStatus(login.LoginID, "", DeviceLoginError, err.Error())
				return
			}

			s.setStatus(login.LoginID, "", DeviceLoginReady, "")
			return
		}
	}
}

// Close stops pending polling requests and waits for their goroutines to exit.
// It is safe to call more than once.
func (s *DeviceLoginService) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *DeviceLoginService) setStatus(loginID string, expected, status DeviceLoginStatus, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	login, ok := s.logins[loginID]
	if !ok || (expected != "" && login.Status != expected) {
		return
	}
	login.Status = status
	login.Error = message
}

func (s *DeviceLoginService) DeleteExpired(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, login := range s.logins {
		if login.ExpiresAt.Before(now) && login.Status != DeviceLoginPending {
			delete(s.logins, id)
		}
	}
}
