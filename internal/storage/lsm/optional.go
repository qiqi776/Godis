package lsm

type Options struct {
    SyncWrites bool
}

type Option func(*Options) error

func WithSyncWrites(enabled bool) Option {
    return func(opts *Options) error {
        opts.SyncWrites = enabled
        return nil
    }
}

func applyOptions(options []Option) (Options, error) {
    var opts Options
    for _, option := range options {
        if option == nil {
            continue
        }
        if err := option(&opts); err != nil {
            return Options{}, err
        }
    }
    return opts, nil
}